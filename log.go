package gorch

import (
	"fmt"
	"os"
	"time"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "????"
	}
}

// config holds orchestrator configuration accumulated from Option functions.
type logEntry struct {
	time    time.Time
	level   LogLevel
	service string
	msg     string
	args    []any
}

// logPump drains the log channel until logQuit is closed, then signals done.
// The channels are passed in rather than read from the Orchestrator so a
// best-effort reset that clears the orchestrator's fields while the pump is
// still winding down cannot race the pump.
func (o *Orchestrator) logPump(logCh chan logEntry, logQuit, done chan struct{}) {
	defer close(done)
	for {
		select {
		case entry := <-logCh:
			o.emitLog(entry)
		case <-logQuit:
			// Drain remaining buffered entries, then exit.
			for {
				select {
				case entry := <-logCh:
					o.emitLog(entry)
				default:
					return
				}
			}
		}
	}
}

// emitLog formats and writes a single log entry to stderr, respecting the
// configured minimum log level.
func (o *Orchestrator) emitLog(entry logEntry) {
	if entry.level < o.cfg.LogLevel {
		return
	}
	levelStr := entry.level.String()
	ts := entry.time.Format("2006-01-02 15:04:05.000")
	var argsStr string
	for i := 0; i < len(entry.args)-1; i += 2 {
		if i > 0 {
			argsStr += " "
		}
		argsStr += fmt.Sprintf("%v=%v", entry.args[i], entry.args[i+1])
	}
	if len(entry.args)%2 != 0 && len(entry.args) > 0 {
		if argsStr != "" {
			argsStr += " "
		}
		argsStr += fmt.Sprintf("%v=(missing)", entry.args[len(entry.args)-1])
	}
	_, _ = fmt.Fprintf(os.Stderr, "%s %-5s %s --- %s %s\n",
		ts, levelStr, entry.service, entry.msg, argsStr)
}

// runService starts a non-cron service. handleServiceDone is called exactly once
// via defer — covering both normal return and panic recovery paths.

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
}

// ServiceLogger — a logger that doesn't log; it sends entries to gorch's log channel.
// gorch consumes the channel and does the actual output (formatting, writing to stderr).
// When a custom Logger is set via Config.Logger, ServiceLogger delegates to it
// instead of the channel, prepending "service"=<name> to the key-value pairs.
//
// Emit never blocks. The channel send is non-blocking: an entry is dropped when
// the log channel is full or the log-pump has been signalled to stop, so a
// torn-down or backed-up log channel can never stall a service or a Stop.
//
// A service hot-added while a Start is in flight may take a logger bound to that
// Start's log channel. If the Start fails, the channel's pump exits during the
// rollback and the survivor's entries are dropped (never buffered by a blocked
// send) until the next successful Start rebinds its logger. Start reassigns a
// logger to every entry in its snapshot, which includes a survivor from a
// previous failed Start, so the drop window ends at the retry.
type ServiceLogger struct {
	svcName  string
	ch       chan<- logEntry // used by default channel-based logging
	quit     <-chan struct{} // closed at shutdown; emit drops entries once closed
	minLevel LogLevel        // entries below this level are dropped at emit
	logger   Logger          // custom logger (bypasses channel)
}

// newServiceLogger creates a channel-based ServiceLogger (internal).
func newServiceLogger(svcName string, ch chan<- logEntry, quit <-chan struct{}, minLevel LogLevel) *ServiceLogger {
	return &ServiceLogger{svcName: svcName, ch: ch, quit: quit, minLevel: minLevel}
}

// newServiceLoggerWith creates a ServiceLogger that delegates to a custom Logger.
func newServiceLoggerWith(svcName string, logger Logger) *ServiceLogger {
	return &ServiceLogger{svcName: svcName, logger: logger}
}

func (l *ServiceLogger) Info(msg string, args ...any)  { l.emit(LogLevelInfo, msg, args) }
func (l *ServiceLogger) Error(msg string, args ...any) { l.emit(LogLevelError, msg, args) }
func (l *ServiceLogger) Debug(msg string, args ...any) { l.emit(LogLevelDebug, msg, args) }
func (l *ServiceLogger) Warn(msg string, args ...any)  { l.emit(LogLevelWarn, msg, args) }

func (l *ServiceLogger) emit(level LogLevel, msg string, args []any) {
	if l.logger != nil {
		fullArgs := make([]any, 0, len(args)+2)
		fullArgs = append(fullArgs, "service", l.svcName)
		fullArgs = append(fullArgs, args...)
		switch level {
		case LogLevelInfo:
			l.logger.Info(msg, fullArgs...)
		case LogLevelError:
			l.logger.Error(msg, fullArgs...)
		case LogLevelDebug:
			l.logger.Debug(msg, fullArgs...)
		case LogLevelWarn:
			l.logger.Warn(msg, fullArgs...)
		}
		return
	}
	// Drop below-minimum entries at emit so a Debug flood cannot starve the
	// log-pump buffer and cause INFO/ERROR entries to be dropped.
	if level < l.minLevel {
		return
	}
	select {
	case l.ch <- logEntry{time: time.Now(), level: level, service: l.svcName, msg: msg, args: args}:
	case <-l.quit: // shutdown in progress: drop rather than risk blocking a Stop
	default: // drop if channel full
	}
}
