# Phase 0 — Foundations

Unblocks everything. No dependencies; all three items can run in parallel.

**X1 — Build fixes landed, submodule on fork branch**
The `Logger::Console { format: None }` bridge fix and the submodule bump to `6e90e6d5`
are committed as `ec200384`.
Remaining: the submodule checkout is on a detached HEAD with a single remote `origin`
pointing at `https://github.com/temporalio/sdk-core.git` — upstream under the wrong
name, which is what the global pre-push hook rejects. Rename that remote to `upstream`
and set its push URL to `DISABLE`, add `origin` = `git@github.com:mfateev/sdk-core.git`,
create `task/python-sdk-streaming` in the submodule, push it, then repoint
`sdk-python/.gitmodules` (currently `https://github.com/temporalio/sdk-rust.git`) and
the submodule pointer at the fork branch.
*Done when:* clean `cargo check` + `maturin develop` + unit tests from a fresh state,
and the submodule resolves to a pushed fork commit rather than a floating local one.
*Note:* never use `--no-verify`; fix the remote naming instead.

**X2 — Redis available and fixtured**
Server install, `redis>=5,<9` in the `dev` dependency group, and `./start-env.sh` are
done. Remaining: a pytest fixture giving each test an isolated keyspace (separate DB
index or key prefix) against the one shared server, so tests can run in parallel under
`pytest-xdist` without colliding.
*Done when:* a trivial test XADDs and XREADs through the fixture, from a clean `uv sync`.

**X3 — Plan dependency-graph and schedule checker**
`tools/check_plan_graph.py` parses the deliverable headers, milestone `**Members:**`
lines, and the `plan-order` stage block across this plan directory, and fails on the
seven conditions tabulated in this directory's `README.md`. It runs on the plan itself, not
on product code, because the failure it prevents is a plan that reads as ordered while
its declared edges say otherwise.
**Scope is deliberately narrow and must stay accurately described**: no prose parsing,
no `Done when` interpretation, no judgment about whether a dependency list is
semantically complete. `--audit-references` is advisory output only and never changes
the exit code.
`tools/test_check_plan_graph.py` is part of this deliverable, not an extra: it mutates
a copy of the real plan once per failure class and asserts the specific message, plus a
valid-plan case that fails if the parse silently found nothing.
*Done when:* the checker exits 0 on this plan directory; its test suite passes with a case for
every one of the seven failure classes, including a schedule that places a deliverable
beside or before a dependency; and every check the plan or `TASK_STATUS.md` attributes
to it is one it actually performs.
