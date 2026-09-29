package gorch

import (
	"errors"
	"testing"
	"time"
)

// ── Dependents / DependenciesOf introspection (issue #31) ──

// TestErrHasDependents_CarriesDependents pins the typed payload: a refused stop
// returns a *HasDependentsError whose Name is the target and whose Dependents
// are the transitive hard dependents that block it, in reverse topological
// order (the most dependent first, the target omitted).
func TestErrHasDependents_CarriesDependents(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("mid"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("top"), DependsOn("mid")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"base", "mid", "top"} {
		if err := o.StartService(name); err != nil {
			t.Fatalf("StartService(%s): %v", name, err)
		}
	}

	err := o.StopService("base", time.Second)
	var depErr *HasDependentsError
	if !errors.As(err, &depErr) {
		t.Fatalf("StopService(base) = %v, want *HasDependentsError", err)
	}
	if depErr.Name != "base" {
		t.Errorf("HasDependentsError.Name = %q, want base", depErr.Name)
	}
	want := []string{"top", "mid"}
	if !equalStrings(depErr.Dependents, want) {
		t.Errorf("HasDependentsError.Dependents = %v, want %v (reverse topological)", depErr.Dependents, want)
	}
	// The typed payload is the same set the query returns.
	got, err := o.Dependents("base")
	if err != nil {
		t.Fatalf("Dependents(base): %v", err)
	}
	if !equalStrings(got, want) {
		t.Errorf("Dependents(base) = %v, want %v (same set as the error)", got, want)
	}
}

// TestErrHasDependents_StillMatchesSentinel pins the compatibility contract: the
// typed error keeps matching the bare sentinel, so a caller that only classifies
// ErrHasDependents does not break when it starts reading the payload.
func TestErrHasDependents_StillMatchesSentinel(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"base", "dep"} {
		if err := o.StartService(name); err != nil {
			t.Fatalf("StartService(%s): %v", name, err)
		}
	}

	err := o.StopService("base", time.Second)
	if !errors.Is(err, ErrHasDependents) {
		t.Fatalf("errors.Is(%v, ErrHasDependents) = false, want true", err)
	}
	// A bare sentinel rendered through the typed error still reads the same.
	if err == nil || err.Error() == "" {
		t.Fatal("refused stop returned an empty error")
	}
}

// TestDependents_ReverseTopologicalOrder pins the ordering: every dependent
// precedes the dependency it reaches through DependsOn. The diamond is asserted
// exactly (the DFS order is deterministic) and the chain checks transitivity.
func TestDependents_ReverseTopologicalOrder(t *testing.T) {
	t.Run("diamond", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		// db <- api <- metrics, db <- worker.
		if err := o.Register(&namedSvc{}, WithName("db")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("api"), DependsOn("db")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("worker"), DependsOn("db")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("metrics"), DependsOn("api")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"db", "api", "worker", "metrics"} {
			if err := o.StartService(name); err != nil {
				t.Fatalf("StartService(%s): %v", name, err)
			}
		}

		got, err := o.Dependents("db")
		if err != nil {
			t.Fatalf("Dependents(db): %v", err)
		}
		// metrics (depends on api) before api; api and worker both before db.
		want := []string{"metrics", "api", "worker"}
		if !equalStrings(got, want) {
			t.Fatalf("Dependents(db) = %v, want %v", got, want)
		}
	})

	t.Run("chain", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("mid"), DependsOn("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("top"), DependsOn("mid")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"base", "mid", "top"} {
			if err := o.StartService(name); err != nil {
				t.Fatalf("StartService(%s): %v", name, err)
			}
		}

		got, err := o.Dependents("base")
		if err != nil {
			t.Fatalf("Dependents(base): %v", err)
		}
		if !equalStrings(got, []string{"top", "mid"}) {
			t.Fatalf("Dependents(base) = %v, want [top mid]", got)
		}
	})

	t.Run("leaf is empty", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		if err := o.Register(&namedSvc{}, WithName("lonely")); err != nil {
			t.Fatal(err)
		}
		if err := o.StartService("lonely"); err != nil {
			t.Fatal(err)
		}
		got, err := o.Dependents("lonely")
		if err != nil {
			t.Fatalf("Dependents(lonely): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Dependents(lonely) = %v, want empty", got)
		}
	})
}

// TestDependents_ExcludesSoftDependents pins the v0.7.0 contract: a soft
// dependent is neither returned by Dependents nor able to block a plain stop,
// and a soft edge does not bridge a transitive hard walk.
func TestDependents_ExcludesSoftDependents(t *testing.T) {
	t.Run("soft sibling of a hard dependent", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("hard"), DependsOn("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("soft"), DependsOnSoft("base")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"base", "hard", "soft"} {
			if err := o.StartService(name); err != nil {
				t.Fatalf("StartService(%s): %v", name, err)
			}
		}

		got, err := o.Dependents("base")
		if err != nil {
			t.Fatalf("Dependents(base): %v", err)
		}
		if !equalStrings(got, []string{"hard"}) {
			t.Fatalf("Dependents(base) = %v, want [hard] (soft excluded)", got)
		}
	})

	t.Run("soft dependent does not block a stop", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("soft"), DependsOnSoft("base")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"base", "soft"} {
			if err := o.StartService(name); err != nil {
				t.Fatalf("StartService(%s): %v", name, err)
			}
		}

		got, err := o.Dependents("base")
		if err != nil {
			t.Fatalf("Dependents(base): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("Dependents(base) = %v, want empty for a soft-only dependent", got)
		}
		if err := o.StopService("base", time.Second); err != nil {
			t.Fatalf("StopService(base) with a running soft dependent = %v, want nil", err)
		}
	})

	t.Run("soft edge does not bridge a transitive walk", func(t *testing.T) {
		o := startedOrchestrator(t)
		t.Cleanup(func() { _ = o.Stop(time.Second) })

		// base <- mid (hard), and mid <- soft-top (soft): soft-top must not be
		// reported as a dependent of base.
		if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("mid"), DependsOn("base")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("soft-top"), DependsOnSoft("mid")); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"base", "mid", "soft-top"} {
			if err := o.StartService(name); err != nil {
				t.Fatalf("StartService(%s): %v", name, err)
			}
		}

		got, err := o.Dependents("base")
		if err != nil {
			t.Fatalf("Dependents(base): %v", err)
		}
		if !equalStrings(got, []string{"mid"}) {
			t.Fatalf("Dependents(base) = %v, want [mid] (soft-top not bridged)", got)
		}
	})
}

// TestDependents_UnknownName_ReturnsErrServiceNotFound pins the not-found
// contract: a query for an unknown name is an error, never a silent empty set.
func TestDependents_UnknownName_ReturnsErrServiceNotFound(t *testing.T) {
	o := New()
	got, err := o.Dependents("ghost")
	if !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("Dependents(ghost) error = %v, want ErrServiceNotFound", err)
	}
	if got != nil {
		t.Fatalf("Dependents(ghost) = %v, want nil", got)
	}
}

// TestDependents_AgreesWithGuard pins that the query and the guard answer the
// same question: for every ServiceStatus of the dependent, Dependents is
// non-empty exactly when a plain stop is refused, and when refused the typed
// error carries that same set.
func TestDependents_AgreesWithGuard(t *testing.T) {
	statuses := []ServiceStatus{
		StatusRegistered,
		StatusStarting,
		StatusRunning,
		StatusStopping,
		StatusStopped,
		StatusCrashed,
		StatusSucceeded,
	}
	for _, st := range statuses {
		t.Run(st.String(), func(t *testing.T) {
			o := New(WithHealthChecksDisabled())
			if err := o.Register(&namedSvc{}, WithName("base")); err != nil {
				t.Fatal(err)
			}
			if err := o.Register(&namedSvc{}, WithName("dep"), DependsOn("base")); err != nil {
				t.Fatal(err)
			}
			if err := o.Start(); err != nil {
				t.Fatal(err)
			}
			defer o.Stop(time.Second)

			// White-box, deterministic status so every row is exercised without
			// racing the lifecycle.
			o.setStatus(entryNamed(t, o, "dep"), st)

			got, err := o.Dependents("base")
			if err != nil {
				t.Fatalf("Dependents(base): %v", err)
			}

			stopErr := o.StopService("base", time.Second)
			refused := errors.Is(stopErr, ErrHasDependents)
			if refused != (len(got) > 0) {
				t.Fatalf("status %s: Dependents = %v, StopService refused = %v; want agreement", st, got, refused)
			}
			if refused {
				var depErr *HasDependentsError
				if !errors.As(stopErr, &depErr) {
					t.Fatalf("status %s: refused stop = %v, want *HasDependentsError", st, stopErr)
				}
				if !equalStrings(depErr.Dependents, got) {
					t.Errorf("status %s: error Dependents = %v, Dependents() = %v; want the same set", st, depErr.Dependents, got)
				}
			}
		})
	}
}

// TestDependenciesOf_DirectHardDependencies pins the opposite direction: the
// direct hard dependencies in declaration order, soft ones excluded, and an
// empty result for a service without dependencies.
func TestDependenciesOf_DirectHardDependencies(t *testing.T) {
	t.Run("declaration order, soft excluded", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("b")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("c")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("d")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("a"), DependsOn("c", "b"), DependsOnSoft("d")); err != nil {
			t.Fatal(err)
		}

		got, err := o.DependenciesOf("a")
		if err != nil {
			t.Fatalf("DependenciesOf(a): %v", err)
		}
		if !equalStrings(got, []string{"c", "b"}) {
			t.Fatalf("DependenciesOf(a) = %v, want [c b] (declaration order, soft excluded)", got)
		}
	})

	t.Run("no dependencies is empty", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("solo")); err != nil {
			t.Fatal(err)
		}
		got, err := o.DependenciesOf("solo")
		if err != nil {
			t.Fatalf("DependenciesOf(solo): %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("DependenciesOf(solo) = %v, want empty", got)
		}
	})

	t.Run("returns a copy", func(t *testing.T) {
		o := New()
		if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
			t.Fatal(err)
		}
		if err := o.Register(&namedSvc{}, WithName("a"), DependsOn("dep")); err != nil {
			t.Fatal(err)
		}
		first, err := o.DependenciesOf("a")
		if err != nil {
			t.Fatal(err)
		}
		first[0] = "mutated"
		second, err := o.DependenciesOf("a")
		if err != nil {
			t.Fatal(err)
		}
		if second[0] != "dep" {
			t.Fatalf("DependenciesOf(a) after mutating the first result = %v, want [dep] (must return a copy)", second)
		}
	})
}

// TestDependenciesOf_UnknownName_ReturnsErrServiceNotFound pins the not-found
// contract for the forward direction.
func TestDependenciesOf_UnknownName_ReturnsErrServiceNotFound(t *testing.T) {
	o := New()
	got, err := o.DependenciesOf("ghost")
	if !errors.Is(err, ErrServiceNotFound) {
		t.Fatalf("DependenciesOf(ghost) error = %v, want ErrServiceNotFound", err)
	}
	if got != nil {
		t.Fatalf("DependenciesOf(ghost) = %v, want nil", got)
	}
}

// TestDependenciesOf_NamesUnregisteredDependency pins that the forward query
// reports the entry's configured dependency even after that dependency was
// removed from the graph, so a caller sees the edge that would make the next
// StartService fail.
func TestDependenciesOf_NamesUnregisteredDependency(t *testing.T) {
	o := startedOrchestrator(t)
	t.Cleanup(func() { _ = o.Stop(time.Second) })

	if err := o.Register(&namedSvc{}, WithName("dep")); err != nil {
		t.Fatal(err)
	}
	if err := o.Register(&namedSvc{}, WithName("a"), DependsOn("dep")); err != nil {
		t.Fatal(err)
	}
	// "a" is Registered, not Running, so removing its dependency is not blocked.
	if err := o.Unregister("dep", time.Second); err != nil {
		t.Fatalf("Unregister(dep): %v", err)
	}

	got, err := o.DependenciesOf("a")
	if err != nil {
		t.Fatalf("DependenciesOf(a): %v", err)
	}
	if !equalStrings(got, []string{"dep"}) {
		t.Fatalf("DependenciesOf(a) = %v, want [dep] even after dep was unregistered", got)
	}
}

// TestHasDependentsError_FormatAndMatch pins the rendered message and the
// standalone errors.Is behaviour of the typed error, independent of a running
// orchestrator.
func TestHasDependentsError_FormatAndMatch(t *testing.T) {
	err := &HasDependentsError{Name: "db", Dependents: []string{"api", "worker"}}

	want := "gorch: service has active dependents: db is depended on by api, worker"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, ErrHasDependents) {
		t.Error("errors.Is(err, ErrHasDependents) = false, want true")
	}
	if err.Is(ErrStopTimeout) {
		t.Error("err.Is(ErrStopTimeout) = true, want false")
	}
	if errors.Is(ErrHasDependents, err) {
		t.Error("errors.Is(ErrHasDependents, err) = true, want false (the sentinel is not the typed error)")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
