package gorch

import (
	"fmt"
	"testing"
	"time"
)

// This file pins the cron grammar gorch accepts and the invariant that the
// registration-time parser (validateCronSpec) and the scheduling parser (the
// live scheduler built by newCronScheduler) are one and the same (issue #34).

// cronSpecCase is one row of the shared grammar corpus. valid records the
// intended accept/reject decision, so the tests below pin the grammar itself
// rather than merely that two parsers happen to agree with each other. why
// documents what the row exercises.
type cronSpecCase struct {
	spec  string
	valid bool
	why   string
}

// cronSpecCorpus is the accepted grammar, expressed as data. It is the
// important artefact of issue #34: it pins the bounds of every field, the
// optional time-zone prefix, the descriptor forms, and the exact field count,
// none of which was previously asserted anywhere. The equivalence and
// schedulability tests both run this corpus.
var cronSpecCorpus = []cronSpecCase{
	// Accepted: six-field expressions (seconds included).
	{spec: "* * * * * *", valid: true, why: "every second"},
	{spec: "*/5 * * * * *", valid: true, why: "step over seconds"},
	{spec: "0 0 0 1 1 *", valid: true, why: "midnight on Jan 1"},
	{spec: "0 0 12 * * MON-FRI", valid: true, why: "named week-day range, upper-case"},
	{spec: "0 0 0 1 jan *", valid: true, why: "lower-case month name"},
	{spec: "0 0 0 * * 0", valid: true, why: "numeric Sunday"},
	{spec: "0 59 23 31 12 6", valid: true, why: "every field at its upper bound"},
	{spec: "0 0 0 1 1 0", valid: true, why: "every field at its lower bound"},
	{spec: "   0 0 0 1 1 *   ", valid: true, why: "surrounding whitespace is ignored"},
	// Accepted: descriptors.
	{spec: "@every 5s", valid: true, why: "sub-minute interval"},
	{spec: "@every 1m", valid: true, why: "minute interval"},
	{spec: "@every 1h30m", valid: true, why: "compound duration"},
	{spec: "@every 0s", valid: true, why: "clamped to one second by the parser"},
	{spec: "@hourly", valid: true, why: "hourly descriptor"},
	{spec: "@daily", valid: true, why: "daily descriptor"},
	{spec: "@midnight", valid: true, why: "midnight descriptor alias"},
	{spec: "@weekly", valid: true, why: "weekly descriptor"},
	{spec: "@monthly", valid: true, why: "monthly descriptor"},
	{spec: "@yearly", valid: true, why: "yearly descriptor"},
	{spec: "@annually", valid: true, why: "annually descriptor alias"},
	// Accepted: explicit time-zone prefix.
	{spec: "CRON_TZ=UTC 0 0 * * * *", valid: true, why: "CRON_TZ prefix"},
	{spec: "TZ=UTC 0 0 * * * *", valid: true, why: "TZ prefix"},
	// Rejected: field count and shape.
	{spec: "", valid: false, why: "empty spec is not a cron service"},
	{spec: "   ", valid: false, why: "blank spec"},
	{spec: "* * * * *", valid: false, why: "five fields, seconds missing"},
	{spec: "* * * * * * *", valid: false, why: "seven fields, no year field supported"},
	{spec: "not a spec", valid: false, why: "non-numeric field"},
	// Rejected: values outside the field bounds.
	{spec: "60 * * * * *", valid: false, why: "seconds above 59"},
	{spec: "* 60 * * * *", valid: false, why: "minutes above 59"},
	{spec: "* * 24 * * *", valid: false, why: "hours above 23"},
	{spec: "* * * 0 * *", valid: false, why: "day-of-month below 1"},
	{spec: "* * * 32 * *", valid: false, why: "day-of-month above 31"},
	{spec: "* * * * 0 *", valid: false, why: "month below 1"},
	{spec: "* * * * 13 *", valid: false, why: "month above 12"},
	{spec: "* * * * * 7", valid: false, why: "day-of-week above 6"},
	{spec: "* * * * * 1-8", valid: false, why: "range end above its bound"},
	{spec: "*/0 * * * * *", valid: false, why: "zero step"},
	{spec: "* * * * * MONDAY", valid: false, why: "full week-day name, only three-letter names"},
	// Rejected: malformed descriptors and time zones.
	{spec: "@every", valid: false, why: "descriptor without a duration"},
	{spec: "@every nope", valid: false, why: "duration that does not parse"},
	{spec: "@unknown", valid: false, why: "unknown descriptor"},
	{spec: "@daily extra", valid: false, why: "descriptor with trailing text"},
	{spec: "CRON_TZ=Nowhere/Fake 0 0 * * * *", valid: false, why: "unknown time zone"},
}

// TestCronSpecParser_MatchesSchedulerParser pins that validateCronSpec accepts
// exactly what the live scheduler accepts. Both paths run the shared corpus and
// must agree, so the grammar accepted at Register time and the grammar the
// scheduler can actually install cannot drift apart (issue #34).
func TestCronSpecParser_MatchesSchedulerParser(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	sched := o.cronSched
	if sched == nil {
		t.Fatal("Start did not install a cron scheduler")
	}

	for i, tc := range cronSpecCorpus {
		t.Run(fmt.Sprintf("case_%02d", i), func(t *testing.T) {
			validateErr := validateCronSpec(tc.spec)
			id, schedErr := sched.AddFunc(tc.spec, func() {})

			if (validateErr == nil) != (schedErr == nil) {
				t.Fatalf("validateCronSpec(%q) error %v and scheduler error %v disagree", tc.spec, validateErr, schedErr)
			}
			if tc.valid {
				if validateErr != nil {
					t.Errorf("validateCronSpec(%q) = %v, want nil (%s)", tc.spec, validateErr, tc.why)
				}
				if schedErr != nil {
					t.Errorf("scheduler rejected %q: %v (%s)", tc.spec, schedErr, tc.why)
				}
			} else {
				if validateErr == nil {
					t.Errorf("validateCronSpec(%q) = nil, want error (%s)", tc.spec, tc.why)
				}
				if schedErr == nil {
					t.Errorf("scheduler accepted %q, want error (%s)", tc.spec, tc.why)
				}
			}
			if schedErr == nil {
				sched.Remove(id)
			}
		})
	}
}

// TestValidateCronSpec_AcceptingSpecIsSchedulable pins the direction of the
// invariant that matters when a hot add is staged: every spec validateCronSpec
// accepts can actually be scheduled by the live scheduler, so a spec that
// passes Register can never fail later inside StartService (issue #34).
func TestValidateCronSpec_AcceptingSpecIsSchedulable(t *testing.T) {
	o := New(WithHealthChecksDisabled())
	if err := o.Start(); err != nil {
		t.Fatal(err)
	}
	defer o.Stop(time.Second)

	accepted := 0
	for _, tc := range cronSpecCorpus {
		if validateCronSpec(tc.spec) != nil {
			continue
		}
		accepted++
		id, err := o.cronSched.AddFunc(tc.spec, func() {})
		if err != nil {
			t.Errorf("validateCronSpec accepted %q (%s) but the scheduler rejected it: %v", tc.spec, tc.why, err)
			continue
		}
		o.cronSched.Remove(id)
	}
	if accepted == 0 {
		t.Fatal("corpus contains no accepted spec; the schedulability assertion would be vacuous")
	}
}
