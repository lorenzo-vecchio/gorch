package gorch

import "fmt"

// Register adds a service to the orchestrator.
//
// Before Start the registry is static: any service may be added, and the whole
// graph is validated at once. After Start (a "hot add") the service is accepted
// into the live graph as StatusRegistered but is NOT auto-started; use
// StartService to start it. While Stop is in progress Register returns
// ErrOrchestratorStopping, and after Stop it returns ErrOrchestratorStopped.
//
// Returns ErrDuplicateName if WithName conflicts with another service.
// Returns ErrInvalidCron if a WithCron spec is empty or malformed, on both the
// static and the hot-add path, so the same spec is accepted or rejected
// identically regardless of when it is registered.
// Returns ErrDependencyCycle if DependsOn introduces a cycle.
// Returns ErrDependencyNotFound if DependsOn names a service that is not yet
// registered: a hard dependency must always be registered before the service
// that names it, both statically and on a hot add.
// Returns ErrDependencyRemoving if a hot add names a hard dependency that is
// being removed (stopped/removed concurrently): a retryable "not now"
// condition, distinct from ErrHasDependents.
// Returns ErrNilService if svc is nil.
// Returns ErrDependencyDepthExceeded if walking the dependency graph to check
// for a cycle runs deeper than 10000 edges: the graph has outgrown
// the registration-time assumption of a bounded, startup-sized acyclic graph,
// and the walk stops with a typed error instead of overflowing the stack.
//
// The caller's RegisterOption closures are applied exactly once, outside the
// orchestrator lock, before any structural validation. A non-idempotent option
// therefore runs once whether or not svc implements Validator.
// Thread-safe.
func (o *Orchestrator) Register(svc Service, opts ...RegisterOption) error {
	o.ensureInit()
	if svc == nil {
		return fmt.Errorf("%w: Register called with a nil Service", ErrNilService)
	}
	// Apply caller options exactly once, before taking any lock. RegisterOption
	// is an arbitrary user closure over mutable state, so it must run once and
	// under no orchestrator lock. The graph-dependent validation happens later
	// in validateRegisterConfigLocked.
	cfg := applyRegisterOptions(opts)

	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return o.registerDynamicCfg(svc, cfg)
	}

	// Static registration is pre-Start. Validate is user code: it must never run
	// under o.mu (a blocking or reentrant Validator would deadlock), matching the
	// dynamic path. The common no-validator case stays entirely under the lock.
	if _, ok := svc.(Validator); !ok {
		cfg, err := o.validateRegisterConfigLocked(cfg)
		if err != nil {
			o.mu.Unlock()
			return err
		}
		o.appendStaticEntryLocked(svc, cfg)
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	if err := callErr(svc.(Validator).Validate); err != nil {
		return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	// Start may have won the race while Validate ran: the service is now a live
	// hot add, appended as StatusRegistered (never auto-started). Validation has
	// already run, so it is not repeated.
	if o.started {
		cfg, err := o.validateRegisterConfigLocked(cfg)
		if err != nil {
			return err
		}
		return o.commitDynamicLocked(svc, cfg)
	}
	cfg, err := o.validateRegisterConfigLocked(cfg)
	if err != nil {
		return err
	}
	o.appendStaticEntryLocked(svc, cfg)
	return nil
}

// appendStaticEntryLocked adds a pre-Start entry to the registry. The caller
// must hold o.mu and have validated cfg via validateRegisterConfigLocked.
func (o *Orchestrator) appendStaticEntryLocked(svc Service, cfg registerConfig) {
	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, status: StatusRegistered}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
}

// applyRegisterOptions applies opts to a fresh registerConfig in order, exactly
// once. It is deliberately lock-free and graph-free: RegisterOption is an
// arbitrary user closure over mutable state, so it must not run more than once
// nor under o.mu. The structural validation that needs the graph runs later in
// validateRegisterConfigLocked.
func applyRegisterOptions(opts []RegisterOption) registerConfig {
	cfg := registerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// validateRegisterConfigLocked performs the structural validation of an
// already-applied registerConfig: the self-heal combination check, cron-spec
// validation, auto-naming, duplicate-name detection, hard-dependency existence
// and cycle detection, and soft-dependency cycle detection. It does not call
// user code. The caller must hold o.mu, because the graph is inspected via
// lookupEntry/dependsOnRecursive. A missing hard dependency is reported with
// ErrDependencyNotFound and a malformed cron spec with ErrInvalidCron on both the
// static and the dynamic path, so callers can classify either with errors.Is
// regardless of when they register.
func (o *Orchestrator) validateRegisterConfigLocked(cfg registerConfig) (registerConfig, error) {
	// Self-heal is only wired into the persistent-service path. A cron tick and a
	// runOnce gate never consume the factory, so reject the combination instead
	// of silently ignoring it.
	if cfg.factory != nil && (cfg.cronSpec != "" || cfg.runOnce) {
		return cfg, fmt.Errorf("%w: WithSelfHeal cannot be combined with WithCron or WithRunOnce", ErrUnsupportedOption)
	}

	// Validate the cron spec here, on both the static and the dynamic path, so
	// the same spec is accepted or rejected identically regardless of when it is
	// registered. cronSet is what makes WithCron("") rejectable: the empty string
	// is the "not a cron service" marker, so only the flag distinguishes an
	// explicit empty spec from no WithCron at all.
	if cfg.cronSet {
		if err := validateCronSpec(cfg.cronSpec); err != nil {
			return cfg, err
		}
	}

	// Auto-name if no WithName set.
	o.autoSeq++
	if cfg.name == "" {
		cfg.name = fmt.Sprintf("$%d", o.autoSeq)
	}

	// Validate uniqueness.
	if _, exists := o.nameIndex[cfg.name]; exists {
		return cfg, fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}

	// Validate all dependencies exist and detect cycles.
	for _, dep := range cfg.dependsOn {
		if dep == cfg.name {
			return cfg, fmt.Errorf("%w: service %s depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			return cfg, fmt.Errorf("%w: dependency %q not found for service %s", ErrDependencyNotFound, dep, cfg.name)
		}
		if depEntry.removing.Load() {
			return cfg, fmt.Errorf("%w: dependency %q is being removed for service %s", ErrDependencyRemoving, dep, cfg.name)
		}
		// Check if dep transitively depends on cfg.name (would create a cycle).
		found, err := o.dependsOnRecursive(depEntry, cfg.name, make(map[string]struct{}), 0)
		if err != nil {
			return cfg, err
		}
		if found {
			return cfg, fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	// Soft dependencies: only validate cycles when the target is already
	// registered; missing targets are tolerated by design.
	for _, dep := range cfg.softDependsOn {
		if dep == cfg.name {
			return cfg, fmt.Errorf("%w: service %s soft-depends on itself", ErrDependencyCycle, cfg.name)
		}
		depEntry := o.lookupEntry(dep)
		if depEntry == nil {
			continue
		}
		found, err := o.dependsOnRecursive(depEntry, cfg.name, make(map[string]struct{}), 0)
		if err != nil {
			return cfg, err
		}
		if found {
			return cfg, fmt.Errorf("%w: %s -> %s", ErrDependencyCycle, cfg.name, dep)
		}
	}

	return cfg, nil
}

// registerDynamicCfg adds a service to a running orchestrator using an
// already-applied cfg. The entry is appended as StatusRegistered and is never
// auto-started (D1); persistent and runOnce services wait for StartService. A
// cron entry is added to the live scheduler immediately (so the schedule is
// live) but still reports StatusRegistered until StartService marks it running.
// Validate runs without the orchestrator lock held, so user code can never
// deadlock against a membership op (the structural graph checks above it run
// under the lock).
func (o *Orchestrator) registerDynamicCfg(svc Service, cfg registerConfig) error {
	o.mu.Lock()
	if err := o.membershipGateLocked(); err != nil {
		o.mu.Unlock()
		return err
	}
	cfg, err := o.validateRegisterConfigLocked(cfg)
	if err != nil {
		o.mu.Unlock()
		return err
	}
	o.mu.Unlock()

	// Validate is user code: never invoke it while holding the orchestrator lock.
	if v, ok := svc.(Validator); ok {
		if err := callErr(v.Validate); err != nil {
			return fmt.Errorf("gorch: service %s validation failed: %w", cfg.name, err)
		}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	return o.commitDynamicLocked(svc, cfg)
}

// commitDynamicLocked appends a hot-added entry to a running orchestrator. The
// caller must hold o.mu, and must have run validateRegisterConfigLocked and
// Validator.Validate (if any) first. The entry stays StatusRegistered and is
// never auto-started (D1); a cron entry is scheduled only by StartService, so its
// schedule and status stay consistent until then.
func (o *Orchestrator) commitDynamicLocked(svc Service, cfg registerConfig) error {
	if err := o.membershipGateLocked(); err != nil {
		return err
	}
	if _, exists := o.nameIndex[cfg.name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateName, cfg.name)
	}
	// The cron spec was already validated by validateRegisterConfigLocked, which
	// every caller of commitDynamicLocked runs first; validateCronSpec is not
	// repeated here.
	// Read the log fields under o.mu: a concurrent Start publishes them in one
	// critical section, so reading them without the lock would race.
	entry := &serviceEntry{svc: svc, cfg: cfg, name: cfg.name, status: StatusRegistered}
	if o.cfg.Logger != nil {
		entry.setLogger(newServiceLoggerWith(entry.name, o.cfg.Logger))
	} else {
		entry.setLogger(newServiceLogger(entry.name, o.logCh, o.logQuit, o.cfg.LogLevel))
	}
	o.entries = append(o.entries, entry)
	o.nameIndex[cfg.name] = entry
	return nil
}

// membershipGateLocked rejects dynamic membership once whole-orchestrator
// shutdown has begun. The caller must hold o.mu.
func (o *Orchestrator) membershipGateLocked() error {
	if o.stopping {
		return ErrOrchestratorStopping
	}
	if o.stopped {
		return ErrOrchestratorStopped
	}
	return nil
}

// RegisterFunc registers a closure-based service under the given name.
// Thread-safe.
func (o *Orchestrator) RegisterFunc(name string, startFn func(ctx ServiceContext) error, stopFn func() error, opts ...RegisterOption) error {
	o.ensureInit()
	if startFn == nil {
		return fmt.Errorf("%w: RegisterFunc called with a nil start function", ErrNilService)
	}
	svc := &funcService{startFn: startFn, stopFn: stopFn}
	allOpts := make([]RegisterOption, 0, len(opts)+1)
	allOpts = append(allOpts, WithName(name))
	allOpts = append(allOpts, opts...)
	return o.Register(svc, allOpts...)
}
