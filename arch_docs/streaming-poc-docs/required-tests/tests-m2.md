# Milestone 2 required tests — 17 cases

Multiple streams, `merge`, same-stream subscriptions, and Continue-As-New. Together with
`tests-m1.md`'s 101 cases it partitions the 118 required cases exactly.

Every bullet is one case. If you add or remove one, update the count in this heading; the
gate checks that the two agree.

- Readiness on any one of several streams resets global quiescence.
- One idle stream cannot park the WFT while another stream is active.
- A write fence on only one stream cannot bypass the global idle timer.
- All fenced streams can request immediate complete-set parking.
- An alternating two-stream batch encodes one run per delivery and triggers budget rollover
  rather than exceeding the budget.
- Simultaneously ready streams are coalesced into one activation when observable together.
- Multiple subscriptions preserve the recorded global delivery ordering.
- Two subscriptions to the same stream name each receive every record from their own cursor
  (broadcast), and their cursors commit and restore independently across Continue-As-New.
- Two same-stream subscriptions park, wake, cancel, and Continue-As-New without overwriting each
  other's park intent or cursor, verified against the backend by inspecting both intents.
- Subscriptions configured with different idle timeouts reduce to `min` deterministically, and
  the same reduction is observed on replay.
- A wake Signal for one parked stream causes all streams to be rechecked.
- Continue-As-New restores its initial cursor from History.
- A multi-activation retained Workflow Task carrying both input and output annotations reproduces
  exactly the live drain count, rather than summing two replay drivers.
- Output cursor resume has neither gaps nor duplicates across Workflow Task rollover and
  Continue-As-New.
- Input and output topics with the same user-visible name remain physically isolated.
- Output-flush and input-park races in both orders produce exactly one Workflow Task terminal and
  one output marker: an output winner invalidates the quiescence generation, rolls back every
  installed intent, and forces a replacement task; a park winner stages output before completing.
- A finished output topic survives Continue-As-New in the reserved must-understand header and
  rejects a successor publish before any backend read.
