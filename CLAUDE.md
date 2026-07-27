# gorch — Go Orchestrator Library

## Structure rules
- **One concept per file.** `service.go` for types/interfaces, `gorch.go` for the orchestrator implementation. If a file passes ~400 lines, split by concern.
- **Tests alongside code.** `service_test.go` and `gorch_test.go` in the same `gorch/` package.
- **No package-level globals** except sentinel errors. Everything lives on structs.
- **Exported API first**, unexported helpers at the bottom of each file.
- **Functional options pattern** for configuration — never a config struct with 12 fields.
- **Interfaces are small.** The `Service` interface has exactly 2 methods (`Start`, `Stop`). Don't bloat it.

## Code quality
- **100% test coverage.** Every branch, every error path, every concurrency mode. Use table-driven tests. Test edge cases: double Start, double Stop, Register after Start, cron with invalid spec, self-heal factory panics, Messenger unsubscribe during Publish, log-pump output format, non-blocking send behavior.
- **No panics in library code.** Recover from service panics, log them, never crash the orchestrator.
- **Thread-safety** documented on every exported method that touches shared state.
- **stdlib first.** Only external dependency is `robfig/cron/v3`. No ORM, no config lib, no framework.

## Conventions
- `gofmt` + `go vet` before commit. No exceptions.
- Error messages lowercase, no trailing punctuation.
- Log format: `YYYY-MM-DD HH:mm:ss.ms LEVEL service --- message key=val`
- Commit messages: imperative, short. `<50 char title>, blank line, body if needed.`
- Package name matches directory name: `gorch/` → `package gorch`.

## Ponytail mode
Shortest working diff. No scaffolding for "later." No interface with one implementation. No config for a value that never changes. Mark deliberate simplifications with `ponytail:` comments naming the ceiling and the upgrade path.

## Module
- Path: `github.com/lorenzo-vecchio/gorch`
- Go version: `1.25`
- Dependencies kept at latest versions (`go get @latest`, GH actions at newest major)
