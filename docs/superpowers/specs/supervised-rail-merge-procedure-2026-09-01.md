# Merge — feat/task-23-supervised-subscription-rail → master

> Manual merge procedure. **No automated push / merge / deployment is
> performed by the agent.** This document only records the steps and
> the verified state.

## Current verified state (local, read-only)

```text
branch:               feat/task-23-supervised-subscription-rail
head:                 b1e6002e1a9d18fbd8d10557ce936365f52f7b61
origin/master:        bd6e99158ff5beba0d23dabb9daa2767d2aac69a
local master:         bd6e99158ff5beba0d23dabb9daa2767d2aac69a
merge-base(branch,master): a27833a9b117e2a7e2a56f591642bab2354d79b2
behind master:        0  (worktree master was fast-forwarded to bd6e991)
ahead origin/master:  2
conflicts (merge-tree dry-run): 0
fast-forward possible: NO  (parallel history, will produce merge commit)
provider/model calls:                0
Hermes proxy launches:               0
OAuth/credential/config changes:     0
Telegram / Sheets / n8n / publication: 0
push:                                0
```

## Commits ahead of `origin/master`

```text
b1e6002 docs(pr): refresh G4-follow-up description             [new]
d3453db feat(supervised): add local finalize command
f470a67 test(supervised): reject future attestation timestamps
89fe5c3 docs(supervised): record owner decision APPROVED G4 canary
```

## What this merge changes

9 files / +368/-16 vs `origin/master` for the three feature commits
(`89fe5c3..d3453db`), plus `+237/-0` for `b1e6002` (the description
refresh). The `.pr-description.md` artefact is intentionally part of
the branch so it travels with the PR, but it is **not loaded at
runtime** by the orchestrator.

```text
A  docs/superpowers/specs/supervised-finalize-cli-design.md
A  docs/superpowers/specs/supervised-rail-attestation-drift-design.md
M  docs/superpowers/specs/supervised-rail-g4-canary-approval.md
M  docs/superpowers/specs/supervised-rail-operator-playbook.md
M  src/seo_orchestrator/cli.py
M  src/seo_orchestrator/supervised_rail.py
M  tests/contract/test_supervised_rail.py
M  tests/integration/test_supervised_subscription.py
M  tests/unit/test_runner_cli.py
A  .pr-description.md                                    [only in branch]
```

## Option A — Web UI (recommended)

1. Push the branch from a shell that has `gh` or from a local checkout:

   ```bash
   cd /opt/data/seo-content-orchestrator/.worktrees/task-23-supervised-subscription-rail
   git push origin feat/task-23-supervised-subscription-rail
   ```

   The push is a **non-fast-forward** update because the branch is
   ahead of `origin/feat/task-23-supervised-subscription-rail` by
   2 commits (`d3453db`, `b1e6002`). GitHub will accept the push and
   offer to open or update the PR.

2. Open (or update) the PR at:

   ```text
   https://github.com/dimapret/seo-content-orchestrator/compare/master...feat/task-23-supervised-subscription-rail
   ```

   - **Title**: `feat(supervised): local finalize CLI + attestation drift guard + G4 canary approval`
   - **Body**: copy from `.pr-description.md` in the worktree.

3. On the PR page, click **Merge pull request** → choose **Create a
   merge commit** (NOT squash, NOT rebase). The PR diff has 4 commits
   that together form a single owner-approved canary scope; squashing
   would obscure the boundary re-statement.

4. After merge: locally `cd /opt/data/seo-content-orchestrator && git
   pull --ff-only` to align local master with `origin/master`.

## Option B — Local CLI (no `gh`)

1. Push (required regardless of UI vs CLI):

   ```bash
   git push origin feat/task-23-supervised-subscription-rail
   ```

2. Decide the merge strategy locally:

   ```bash
   cd /opt/data/seo-content-orchestrator
   git checkout master
   git pull --ff-only origin master   # safety
   git merge --no-ff feat/task-23-supervised-subscription-rail \
       -m "Merge pull request #14 from dimapret/feat/task-23-supervised-subscription-rail"
   git push origin master
   ```

   `--no-ff` is intentional: it preserves the G4 canary boundary as a
   single reviewable unit on the master timeline. A fast-forward merge
   is **not possible** here because the branch and master have
   parallel history.

3. Cleanup:

   ```bash
   git push origin --delete feat/task-23-supervised-subscription-rail
   ```

## Boundaries

The merge operation itself is purely a Git ref update plus a GitHub
PR close — it does **not**:

- invoke any provider model;
- launch the Hermes proxy;
- touch OAuth credentials or any `.env` / config;
- publish to Telegram, Google Sheets, n8n, or any external system;
- deploy or restart any runtime.

The supervised rail, after merge, is **still** authorised only for the
single canary run documented in section 11 of
`docs/superpowers/specs/supervised-rail-g4-canary-approval.md`. Any
further run requires a fresh owner-decision phrase, which is **not**
produced by this merge.

## What is NOT closed by this merge

- the canary job `job-bca98b65bc214cd8a5d149223b2dc472` is still
  `RUNNING/outline`; it requires a completion envelope from a visible
  Hermes session before `supervised-bind` and the next packet
  (`draft`) can be prepared;
- there is no production deployment of the SEO Content Orchestrator;
- there is no automation that publishes revisions to a live site
  without manual final review.