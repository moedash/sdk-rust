# ADR-041 — A task on its way to the server keeps the readiness promise it made

**Status:** Accepted · **Affects:** C7, C15b · **Spec:** `spec/core-lang-protocol.md`

## Context

`notify_external_stream_ready` answers one of five results, and `Accepted` is the only one that
tells the watcher to do **nothing**: Core has taken the readiness and will produce the activation
that delivers it. Every other result puts the obligation back on the watcher, which sends the wake
Signal itself.

`Accepted` is returned whenever a Workflow Task is open, and a Workflow Task is open from the moment
it is applied until `mark_wft_complete` runs — which is *after* the server has answered the
completion. Between those two points sits every completion that reports rather than retains: a
replaying one, one carrying a server-bound command, one answering a query. Each registers the wait
set lang described and then reports the task, and neither the registration path nor the report path
turns pending readiness into a `ResolveExternalStreamWaits` job — `_check_more_activations`, which
is where readiness accumulated during an outstanding activation is normally picked up, is not on
either of them.

So a readiness accepted anywhere in that window is stranded. Core holds it in `ready`, no activation
is ever issued for it, the task closes, and the watcher — told to do nothing — owes no Signal. The
record stays buffered behind a Run that Core believes it has already told, and nothing in the system
is waiting for anything: the Workflow is blocked, the Worker is idle, the server has no task to
schedule.

A Run resumed by a wake Signal walks straight into this. Its replayed `subscribe()` starts a watcher
that finds records already in the stream, so readiness arrives during the very activation whose
completion registers the wait set — and that completion refuses retention because the Run was still
replaying when it began.

## Options

**A. Retain the task instead.** Decide retention from the machines' state *after* the completion is
applied, so a completion that catches the Run up holds the task open the way a live one does.

**B. Revoke the accepted readiness** when the task is reported: clear it, put the waits back to
blocked, and let the next report of the same record cover it.

**C. Let the wake Signal cover it** — treat the window as the watcher's problem.

**D. Keep the promise on the next task.** End local delivery when Core commits to reporting, so
readiness arriving *after* that point is answered `NoOpenWorkflowTask` and the watcher signals for
itself; and for readiness already accepted, ask the server for the replacement task the resolve job
will be issued on.

## Decision

**D.**

A is the better shape and is not this change. `replaying` is sampled before lang's commands reach
the machines, so it describes the activation rather than its outcome, and a completion that catches
a Run up is a live completion by the time retention is decided — deciding it there would also make
this window narrower. But retention changes what is written to History: the marker for a boundary
that no longer closes is not emitted, so a Run of this shape records different history than it does
today, and existing histories would replay against a Worker that writes a different command
sequence. That is a versioned change, and it does not remove the need for D — a completion carrying
a server-bound command must be reported however retention is decided.

B is silent loss dressed as bookkeeping. The watcher was told `Accepted`; it has already advanced
past the buffered record and will not report it again. Revoking leaves nobody holding the
obligation, which is the defect, not the fix.

C is what the code did. The watcher owes a Signal only for the results that say so, and `Accepted`
is the one that says the opposite — so nothing sends one.

## Consequences

- Local delivery ends when the completion is prepared for the server, not when the server answers.
  The window in which `Accepted` can be returned is exactly the window in which Core can still act
  on it.
- A completion reported with readiness pending sets `force_create_new_workflow_task`. The
  replacement task re-opens the wait set in `apply_new_wft`, and `_check_more_activations` issues
  the resolve job the pending readiness was promised — no Signal round trip, and no dependence on
  the watcher having kept anything.
- Readiness landing after that point takes the `NoOpenWorkflowTask` path, which is the documented
  healthy state between Workflow Tasks and already sends the wake Signal.
- The extra task is bounded: it is requested only when readiness is actually pending, and the
  activation it carries drains that readiness, so the next completion has nothing to force.
- A test for this cannot be written from a quiescent command alone, because the wait set does not
  exist until the completion that registers it. It needs the set seeded with a task open, readiness
  accepted against it, and then a completion carrying a server-bound command — which is what
  `readiness_accepted_against_a_reported_task_is_not_stranded` does.
