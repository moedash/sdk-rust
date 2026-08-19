# Workflow streaming implementation review — third round

**This is a review artifact, not part of the design.** Like `review-guide.md` and
`follow-up-review.md`, it records what was found and what was done about it. Everything else in this
directory states current truth and carries no revision narrative; the specs and decision records
named below were updated in place, and those are the authority on what the code now does.

Date of review: 2026-08-19 · Reviewed revisions: `sdk-python` `5a887335`, `sdk-rust` `49150bf6`

The review found six defects — five P1 and one P2 — and reported them as static findings, having
been unable to run the suite on the reviewing host: dependency restoration needed unavailable
network access, and the checked-in bridge extension is a Linux ELF binary while that host was macOS.
Every finding was reproduced in the container, where the suite does run, before being fixed.

**Each fix is covered by a test that was confirmed to fail against the pre-fix code**, by reverting
the fix and re-running it — step 4 of "before reporting a defect found by a test" in
`verification-hazards.md`, and the only thing that distinguishes a regression test from a test that
happens to pass. Cases 56-62 of `required-tests/tests-m1.md` are those tests; 63 and 64 cover two
defects the fixes themselves introduced, recorded at the end of this file.

All six are fixed in Python `8abb8eb8`, *Close the third review's six findings, and the seven the
fixes introduced* — one commit, because the byte-budget and segmentation work overlaps and splitting
it would have left intermediate states that fail.

| Severity | Finding | Status |
|---|---|---|
| P1 | Replay-to-live cursor reposition is asynchronous, allowing duplicate delivery | Fixed — `8abb8eb8` |
| P1 | A replay job performs one more event-loop drain than its annotation records | Fixed — `8abb8eb8` |
| P1 | The hard annotation budget can fail permanently instead of forcing rollover | Fixed — `8abb8eb8` |
| P1 | Producer coordination failures after append lose the durable-but-unacknowledged state | Fixed — `8abb8eb8` |
| P1 | Grace-period cancellation drops shutdown wakes without recording the required metric | Fixed — `8abb8eb8` |
| P2 | External-payload storage outages are misclassified as decode failures | Fixed — `8abb8eb8` |

## P1 — Replay-to-live cursor reposition is asynchronous

**Confirmed.** `reposition_to_committed()` posted `_reposition_to_committed` with
`call_soon_threadsafe()` and returned. `_apply_replay_external_streams()` then left replay mode
immediately, so a drain reaching the live buffer before the manager's loop ran its callback found
every marker-covered record still sitting there and delivered it a second time. Nothing ordered the
two. The existing regression test only passed because it executed `await asyncio.sleep(0)` before
looking — the yield production code does not have.

**Fixed by making the retraction synchronous on the Workflow thread**, with only the watcher's
wakeup hopped: retracting touches the buffer, the three uncommitted cursors, and the prefetch epoch,
all under the subscription's lock and none of them an `asyncio` primitive, while `_has_room.set()`
is an `asyncio.Event` and may not be set from another thread.

That move required a second change. The watcher's prefetch-epoch check moved **inside** `_append`,
under the same lock the retraction takes: with the retraction now landing on another thread, a check
made outside the lock could pass and *then* have the retraction land, putting the retracted records
straight back into the buffer and re-advancing `prefetch` past them. Under the lock an append is
either cleared by the retraction or rejected by it, with no third interleaving.

Spec: `spec/python-runtime.md`, "The reposition after a replay is synchronous".

## P1 — A replay job performs one more drain than its annotation records

**Confirmed.** The driver drained all *k* recorded segments, and the activation then ran its own
`_run_once` for the job set the replay job arrived in — *k + 1* drains for the *k* ADR-018 requires.
The unit test drove `_apply_replay_external_streams` through a stub and never ran the surrounding
activation, so the drain it missed was exactly the extra one.

**Fixed by leaving the last segment to the drain the activation was always going to run.** The
driver drains the first *k - 1* and arms the last. This also mirrors the live run, where each
activation's single trailing drain served the records that activation had just been handed. Closing
the replay — the consumed check, the cursor reposition, and leaving replay mode — therefore moved
after that drain, into `_finish_replay_external_streams`, called from the activation's own `finally`
so a drain that raised still leaves replay mode.

Every test in `test_replay.py` now drives the activation's drain as well as `_apply`, through one
helper, because a test that drives only the driver cannot see this class of defect at all.

Spec: `spec/annotation-format.md`, "Activation segmentation"; ADR-018.

## P1 — The hard annotation budget can fail permanently instead of forcing rollover

**Confirmed, and broader than one defect.** A high-water mark is a *fraction* of the budget and
turns true only once a frame that crossed it has been emitted, while frames are indivisible and
three of the four carry strings whose length this side does not choose. Three distinct ways to reach
the hard cap existed:

- `request_rollover` was sampled **before** `take_observation_delta()`, which is what closes the
  activation's segment — so the segment that crossed the mark went out with
  `request_rollover = false` and the next activation was free to add another frame first;
- a segment, bindings, or terminal frame larger than the remaining slack raised
  `AnnotationBudgetExceeded`, and the retry re-encoded the identical bytes: the permanent Workflow
  Task retry loop ADR-007 exists to rule out;
- a header is one frame and a rollover writes a *fresh* one, so enough subscriptions or long enough
  names made the first observation of **every** annotation unencodable.

**Fixed with four rules**, each closing one route:

1. The **closing frames are reserved, not checked.** The terminal, and a bindings frame for any wait
   registered since the header went out, are priced in advance and only they may spend that
   reservation; segments are refused before it is gone. Neither may ever be refused — both record
   something that already happened, and an annotation Core writes with no terminal is durable and
   undecodable past the frame after it.
2. **Delivery stops before a segment the annotation could not record.** The annotation budget is a
   second delivery budget beside the per-activation record cap and the smaller wins. It is spent
   before a record is handed to Workflow code — the last point at which stopping is an option —
   priced per record by the largest run this annotation has actually encoded, floored so the first
   record is costed pessimistically. The activation ends `BUDGET_ROLLOVER`.
3. The **rollover request rides the completion that crossed the line**, read after the segment is
   closed.
4. A subscription set whose **header and terminal alone** cannot fit an empty annotation is refused
   at `subscribe()`, as a deterministic non-retryable `ExternalStreamCapacityError` — the one point
   at which the answer is still "do not make this subscription" rather than "this Workflow Task
   cannot be completed".

`AnnotationBudgetExceeded` remains as the last resort but is now a non-retryable
`ApplicationError`: it fails the Workflow rather than the Workflow Task, because the server retries
Workflow Task failures regardless of cause.

Spec: `spec/annotation-format.md`, "The high-water mark alone is not a bound";
`spec/python-runtime.md`, delivery budget; ADR-007.

## P1 — Producer coordination failures after append lose the unacknowledged state

**Confirmed.** Only the Signal send was inside the guarantee. `parked_wait_ids()`,
`current_park_generation()`, and `claim_park_generation()` sat outside it, so a provider outage in
any of them escaped as whatever the provider raised — a bare `ConnectionError` — straight through
`publish()`, which catches only the durable-but-unacknowledged error. The caller lost the offset with
it, and its obvious move, retrying `publish()`, appended a **second** record: the sequence number had
advanced and the idempotency key with it. `finish_writing()` had the same defect, with a second fence.

**Fixed by putting all three steps inside the guarantee**, and by distinguishing the two recoveries,
because they are not the same:

| Failed at | `pending` | Recovery |
|---|---|---|
| the Signal | the wakes still owed | re-send them verbatim (`retry_wake`) |
| observe or claim | empty, `restart` set | call `wake()` again |

`retry_wake` now refuses an empty list rather than returning quietly: a no-op there looks like
recovery while the record stays durable and unannounced.

Spec: `spec/wake-signal.md`, "All three steps after the append are inside the guarantee".

## P1 — Grace-period cancellation drops shutdown wakes without recording the metric

**Confirmed.** `asyncio.wait_for` cancels `_sweep()` wherever it stands, `_send_owed_wake`
deliberately re-raises `CancelledError`, and the accounting sat after it — so neither the wake the
cancellation landed inside nor any subscription later in the serial loop was counted. Shutdown
reported `shutdown_wake_failures == 0` for a Worker that had just abandoned every one of its
handoffs. A probe that raised was passed over with the same silence.

**Fixed by accounting for subscriptions rather than for attempts.** The sweep begins with every
subscription unaccounted for and resolves each exactly once — its Run's status says nothing is owed,
or a wake was attempted and its result recorded. Whatever remains when the sweep stops is counted, in
a `finally`, so cancellation and never-being-visited are covered by one rule. A Run whose status
probe cannot answer stays unaccounted for on purpose: the sweep still sends it nothing, because its
Workflow Task may be open, but "we could not tell" is not "nothing was owed".

A Run that is parked or has an open Workflow Task is still counted as owing nothing — a metric that
fired on every clean shutdown would be unalertable, and that is asserted too.

Spec: `spec/wft-lifecycle.md`, "The no-open-WFT transition".

## P2 — External-payload storage outages are misclassified as decode failures

**Confirmed.** With external storage configured a record's bytes are a *reference*; the value does
not exist until the payload store hands it over. A driver that cannot reach the store raises whatever
its client raises and the `DataConverter` does not wrap it, so the consumer saw an unclassified
exception on a record whose range validated — and the classification rule turns that into row three.
An operator was told to align a converter that was never wrong, during a transient outage that clears
itself. The taxonomy's own note that such a failure "stays row one" was true of causes that arrive
already labelled, and nothing labelled this one.

**Fixed at the retrieval call inside the preparation step**, and only around that call: the user's
`PayloadCodec` runs immediately afterwards and keeps row three, because a codec that rejects intact
bytes *is* the configuration mismatch.

Spec: `spec/failure-taxonomy.md`, "An unreachable external payload store is row one".

## Three defects the fixes themselves introduced

Reviewing the six fixes found three more, all in code written for this round and all caught before it
closed. They are recorded because the shape is the one worth remembering: each came from a change that
was correct for the case it was aimed at and wrong for an adjacent one -- the same shape the follow-up
review found across the first fifteen findings.

**The byte-budget fix could wedge a Workflow permanently.** Stopping delivery and requesting the
rollover are one mechanism — the first bounds the marker, the second gives the next Workflow Task an
annotation to deliver into — and the first was written as a property of the *activation*
(`_observed_this_activation and affordable <= 0`) rather than of the annotation. Combined with the
other half of the same fix, reading the rollover flag *after* `take_observation_delta()`, that flag was
read after the call that clears `_observed_this_activation`: delivery stopped, no rollover was
requested, and the next activation inherited a full annotation, a delivery budget of zero, and
therefore no way to ever observe anything again. Reproduced with a 1024-byte budget and 300-byte
offsets, where the frame exceeds the slack the fractional high-water mark leaves without reaching the
mark, so the mark could not stand in for the missing condition. Case 63.

**The sweep fix counted abandoned wakes on a manager that had no sweep.** `_sweep` returns early when
no run-status probe is wired, and treating that as "nothing resolved" counted every subscription as a
lost handoff — firing the metric wherever the mechanism simply is not configured, which is noise in
the one series operators are expected to alert on. Case 64.

**The segmentation fix deferred the close for a marker with no segments, and that reintroduced
duplicate delivery.** With no recorded segment for the activation's drain to serve, that drain is a
*live* one: records that arrived while the Run was evicted are already buffered and it hands them
over. Repositioning after it retracted exactly what it had just delivered, and the watcher re-read and
re-delivered records Workflow code already had — the mirror image of the first finding in this round.
A zero-segment marker now closes inside the job, before that drain.

This one is worth dwelling on, because it is the only defect in this round that **the suite found and
inspection did not**, and it presented in the way `verification-hazards.md` warns about: as an
occasional end-to-end failure that passed in isolation. It is timing-dependent by nature — the
activation's drain only has something to deliver if the watcher buffered first, which a loaded machine
gives it time to do and an isolated run does not. Two of three full-suite runs failed on
`test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor`; every isolated run of it
passed. Dismissing that as load flakiness was the wrong call and was made twice before the pattern —
same test, not a different one each time — forced the question. Case 65 reproduces it deterministically
by pre-filling the buffer, which is what the end-to-end test can only do by luck.

## The empty-stream replay flake

A separate handoff, `empty-stream-replay-flake-handoff.md`, traced the intermittent failure of
`test_an_empty_stream_parked_and_evicted_replays_from_the_recorded_cursor` to the first two findings
above and set out six properties a safe fix must have. All six hold, and that document now carries its
own resolution: what was done, the two deterministic tests it asked for, and three corrections to its
analysis — the zero-segment case needing a fix of its own, the bare `finally` masking real errors, and
the flake being **pre-existing** rather than produced by those findings. It also names a test-harness
race, now fixed: the eviction spy recorded a Run before awaiting teardown, so the test published
records while the old watcher was still alive.

## What this round did not change

- No Core (Rust) code. All six findings were Python-side, and Core stays annotation-blind throughout
  — including for the byte-budget work, where every new rule is a Python decision Core is merely told
  the outcome of.
- No new taxonomy row. `ExternalStreamCapacityError` is deliberately outside the four: those describe
  a stream or a converter behaving unexpectedly at read time, and this describes Workflow code asking
  for more than the marker format can carry, known at `subscribe()`.
