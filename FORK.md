# Working on the DigitalOcean fork of llm-d-router

This repository is DigitalOcean's fork of
[llm-d/llm-d-router](https://github.com/llm-d/llm-d-router). We use it to
build and ship features before upstream releases them, and we propose those
features back to upstream one at a time.

Everything in this document is fork-specific. For how the code itself is
built, tested, and structured, see [DEVELOPMENT.md](DEVELOPMENT.md) and
[CONTRIBUTING.md](CONTRIBUTING.md), which come from upstream.

## Contents

- [Ground rules](#ground-rules)
- [Branches](#branches)
- [One-time setup](#one-time-setup)
- [Building a feature in the fork](#building-a-feature-in-the-fork)
- [Fork-only changes](#fork-only-changes)
- [Images and Helm charts](#images-and-helm-charts)
- [Syncing from upstream](#syncing-from-upstream)
- [Contributing a feature upstream](#contributing-a-feature-upstream)
- [Tracking what the fork carries](#tracking-what-the-fork-carries)
- [Repository configuration](#repository-configuration)
- [Troubleshooting](#troubleshooting)

## Ground rules

1. Never commit to `upstream-main`. It is an exact copy of upstream.
2. Sign off every commit (`git commit -s`). Upstream rejects commits without a
   DCO `Signed-off-by` trailer, and rewriting them later is painful.
3. Keep one feature per PR, so each one can be proposed upstream on its own.
4. Squash-merge feature PRs. Merge sync PRs with a merge commit. Never rebase
   or force-push `main`.
5. Put fork-only code in new files rather than editing upstream files wherever
   you can. Every edited upstream line is a potential conflict on every sync.
6. Label every PR `upstream-candidate` or `fork-only` (see
   [Tracking what the fork carries](#tracking-what-the-fork-carries)).

## Branches

| Branch | Purpose | Who writes to it |
|---|---|---|
| `main` | Default branch. Upstream plus our features. Images and charts are published from here. | PRs only |
| `upstream-main` | Exact mirror of `llm-d/llm-d-router:main`. | Fast-forward pushes from upstream only |
| `feat/*`, `fix/*`, ... | Work in progress for the fork. | Anyone |
| `upstream/*` | Branches prepared for an upstream PR. | The feature's author |
| `sync/*` | Upstream syncs that needed conflict resolution. | The sync owner |

## One-time setup

1. Clone the fork and add upstream as a second remote:

   ```bash
   git clone git@github.com:digitalocean/llm-d-router.git
   cd llm-d-router
   git remote add upstream https://github.com/llm-d/llm-d-router.git
   git fetch --all
   ```

2. Make sure the email in your commits is the one you sign off with. The DCO
   check compares the `Signed-off-by` email with the commit author email:

   ```bash
   git config user.name  "Your Name"
   git config user.email "you@digitalocean.com"
   ```

## Building a feature in the fork

1. Branch from the right base:
   - If the feature could go upstream, branch from `origin/upstream-main`. It
     then contains nothing fork-specific and can be proposed unchanged.
   - If it depends on fork-only code, branch from `origin/main`.

   ```bash
   git fetch origin
   git checkout -b feat/my-feature origin/upstream-main
   ```

2. Commit with sign-off: `git commit -s -m "feat(epp): ..."`. Upstream uses
   Conventional Commit-style titles; use them here too so commits can go
   upstream as they are.
3. Push and open a PR into `main`. Fill in the PR template the same way you
   would upstream, including the release note. That keeps the upstream PR
   cheap later.
4. Add the `upstream-candidate` or `fork-only` label.
5. Once it is approved and CI passes, **squash-merge** it. The PR becomes one
   commit on `main`, which is easy to cherry-pick upstream.

Merging to `main` publishes new images and charts within a few minutes (see
[Images and Helm charts](#images-and-helm-charts)).

## Fork-only changes

Some changes will never go upstream: DigitalOcean-specific integrations,
deployment config, and our CI. To keep syncs painless:

- Prefer new files and directories over edits to upstream files. If you must
  edit an upstream file, keep the change small and in one place.
- Name fork workflows `.github/workflows/do-*.yaml`.
- Do not delete or rename upstream workflows. Disable them in the Actions UI
  instead (see [Repository configuration](#repository-configuration)).
  Deleting or renaming the files would conflict on every sync.
- Do not edit `CODEOWNERS`, `.github/PULL_REQUEST_TEMPLATE.md`, or
  `.github/ISSUE_TEMPLATE/*`. Upstream changes them often.

## Images and Helm charts

`.github/workflows/do-publish-images.yaml` runs on every push to `main`, which
includes merged feature PRs and sync PRs. It builds, scans with Trivy,
publishes, and attests:

| Artifact | Reference |
|---|---|
| EPP | `ghcr.io/digitalocean/llm-d-router/endpoint-picker` |
| Disaggregation sidecar | `ghcr.io/digitalocean/llm-d-router/disagg-sidecar` |
| Coordinator | `ghcr.io/digitalocean/llm-d-router/coordinator` |
| Standalone chart | `oci://ghcr.io/digitalocean/llm-d-router/charts/llm-d-router-standalone` |
| Gateway chart | `oci://ghcr.io/digitalocean/llm-d-router/charts/llm-d-router-gateway` |

Tags and versions:

- Images get `:main`, which moves with every merge, and `:<full commit sha>`,
  which never changes.
- Charts get version `0.0.0-main`, which moves, and
  `0.0.0-main-<12-char sha>`, which never changes. Each chart's EPP image is
  pinned to the commit it was built from.
- Running the workflow by hand on another branch publishes the same way, with
  the branch name in place of `main` (`feat/foo` becomes `feat-foo`).

Deploy with an immutable reference. Use `:main` and `0.0.0-main` only for
experiments. Helm ignores prerelease versions unless you name one, so always
pass `--version`:

```bash
helm install router \
  oci://ghcr.io/digitalocean/llm-d-router/charts/llm-d-router-standalone \
  --version 0.0.0-main-<sha>
```

The latency-predictor images referenced by the charts still come from
`ghcr.io/llm-d`. This repository does not build them.

A HIGH or CRITICAL Trivy finding blocks publishing, as it does upstream. If
a new CVE in a base image blocks a release, fix or bump the base image in a
PR, or ideally upstream.

## Syncing from upstream

**Cadence:** at least weekly, and before starting any large feature. Small,
frequent syncs are much easier than occasional large ones.

**Owner:** one person per week, on rotation. They run the sync and resolve or
route any conflicts.

### 1. Update `upstream-main`

```bash
git fetch upstream
git push origin upstream/main:refs/heads/upstream-main
```

This is always a fast-forward. If the push is rejected, someone committed to
`upstream-main` by mistake; see [Troubleshooting](#troubleshooting).

### 2. Open the sync PR

```bash
gh pr create --repo digitalocean/llm-d-router \
  --base main --head upstream-main \
  --title "Sync upstream $(date -u +%F)" \
  --body "Merges llm-d/llm-d-router main into the fork."
```

If GitHub reports no conflicts, wait for CI, then merge it with **Create a
merge commit**. Never squash a sync PR: squashing loses upstream's history and
makes every later sync conflict.

**Expect `dco-check` to fail on sync PRs.** Upstream's merge queue accepts
commits whose author is a GitHub noreply address while the `Signed-off-by`
trailer has the real email. The same check running on a normal PR in the fork
rejects them. These commits were already DCO-checked upstream, so the sync
owner merges sync PRs with the ruleset bypass. Every other required check
should still pass.

### 3. If there are conflicts

Resolve them on a separate branch so `upstream-main` stays a clean mirror:

```bash
git fetch origin
git checkout -b sync/$(date -u +%F) origin/main
git merge origin/upstream-main
# resolve conflicts
git add -A
git commit -s
git push -u origin HEAD
```

Open a PR from `sync/<date>` into `main`, merge it with **Create a merge
commit**, and close the PR from step 2.

When resolving:

- For a feature we already upstreamed, **take upstream's version**. Upstream
  is the source of truth once it merges.
- For fork-only code, keep our change and adapt it to upstream's new code.
  Pull in the author if the conflict is not obvious.
- Run `make test-unit lint` locally before pushing.

### 4. After merging

- Check that the `DO - Publish Images and Charts` run on `main` succeeded.
- Rebase open fork PRs if they now conflict.

## Contributing a feature upstream

Upstream contributions follow upstream's own process: [CONTRIBUTING.md](CONTRIBUTING.md),
[docs/issue_triage.md](docs/issue_triage.md), and the PR template.

### 1. Prepare the upstream branch

Start from the latest upstream, not from our `main`:

```bash
git fetch origin upstream
git checkout -b upstream/my-feature origin/upstream-main
git cherry-pick <squashed commit sha from fork main>
make test-unit lint
git push -u origin upstream/my-feature
```

- Do not use `git cherry-pick -x`. It adds a reference to a fork commit that
  means nothing upstream.
- Check that the commit's `Signed-off-by` email matches its author email. Fix
  it with `git commit --amend -s --no-edit` if needed.
- If the feature depends on fork-only code, remove or replace that dependency
  on this branch. Upstream has to build and pass without our fork.
- If several fork commits make up one feature, cherry-pick them all and squash
  them into one coherent commit, or a small series.

### 2. Open the upstream PR

1. Find or open the upstream issue and comment `/assign` on it. Upstream uses
   the assignee to mark an issue as taken.
2. Open a PR from `digitalocean/llm-d-router:upstream/my-feature` into
   `llm-d/llm-d-router:main`.
3. Fill in upstream's PR template, including the kind and the release note.
   Upstream's CI validates the release note.
4. Link the upstream PR from the original fork PR.

Upstream maintainers cannot push commits to branches in an organization-owned
fork. If a maintainer wants to push fixes directly, push the same branch to
your personal fork and open the PR from there instead.

### 3. During upstream review

- Push review changes as new commits on `upstream/my-feature`. Rebase onto
  upstream when their `needs-rebase` bot asks.
- Do not bring each review change back into the fork. They arrive with the
  sync once upstream merges. If the fork needs a fix urgently, cherry-pick it
  into a normal fork PR.

### 4. After upstream merges

Upstream squashes the PR into a new commit on their `main`. It has a different
sha from our fork commit, and possibly different content after review.

1. On the next sync, the upstream commit arrives in `upstream-main` and then
   in the sync PR.
   - If the content is identical, git merges it cleanly.
   - If review changed it, resolve the conflicts **in favour of upstream's
     version**.
2. After the sync merges, the fork should not differ from upstream for that
   feature:

   ```bash
   git fetch origin
   git diff origin/upstream-main origin/main -- <paths touched by the feature>
   ```

   Any remaining difference is either fork-only code, which is fine, or
   something that should also go upstream.
3. On the fork PR, change the label from `upstream-candidate` to `upstreamed`,
   and delete `upstream/my-feature`.

### If upstream declines the feature

Change the label to `fork-only`. The fork carries the feature indefinitely, so
keep it as isolated as possible (see [Fork-only changes](#fork-only-changes)).

### Upstream first

If a feature is ready from the start, you can open the upstream PR first and
the fork PR in parallel from the same branch. The fork gets the feature right
away, and the end state after the next sync is the same.

## Tracking what the fork carries

Labels on fork PRs:

| Label | Meaning |
|---|---|
| `upstream-candidate` | Should go upstream; not proposed or not merged yet |
| `upstreamed` | Merged upstream; the fork now gets it through syncs |
| `fork-only` | Stays in the fork |

To list commits on our `main` that are not in upstream:

```bash
git fetch origin
git log --oneline --no-merges origin/upstream-main..origin/main
```

Upstreamed features still appear in that list, because upstream's commit has
a different sha. `git cherry -v origin/upstream-main origin/main` marks
commits upstream merged unchanged with `-`. For the rest, rely on the labels.

## Repository configuration

For admins. These settings exist to support the process above.

**General**
- Default branch: `main`.
- Merge methods: squash and merge commits allowed; rebase disabled. Squash
  message: pull request title and commit details.
- Sign-off on web-based commits: required.
- Automatically delete head branches: on.

**Rulesets**
- `main`: no deletions or force pushes. PRs need one approval, and stale
  approvals are dismissed. Required checks are lint, unit tests, and
  `dco-check`. No linear-history requirement, because sync PRs are merge
  commits. No code owner review, because `CODEOWNERS` lists upstream
  maintainers. The sync owners need **bypass** permission (for pull requests)
  so they can merge sync PRs past `dco-check`.
- `upstream-main`: no deletions or force pushes.

**Actions**

Upstream workflows that are disabled in the Actions UI (not deleted):

| Workflow | Reason |
|---|---|
| `ci-dev.yaml` | Publishes to `ghcr.io/llm-d`. Replaced by `do-publish-images.yaml`. |
| `ci-release.yaml` | Publishes upstream releases on tag pushes. |
| `nightly-router-perf-test-*.yaml` | Needs upstream's GKE and Hugging Face secrets. |
| `pr-merge-queue.yaml`, `pr-review-gate.yaml`, `pr-hold-gate.yml`, `non-main-gatekeeper.yml` | Upstream's merge automation. |
| `stale.yaml`, `unstale.yaml`, `issue-triage.yaml`, `issue-assignment-check.yaml`, `issue-kind-label.yaml` | Upstream's issue automation. |

When a sync brings in a new upstream workflow, check whether it should run in
the fork, and disable it if not.

The e2e job in `ci-pr-checks.yaml` needs an `HF_TOKEN` repository secret.

**Packages**

Each image and chart is its own GHCR package, with its own visibility. All
five are linked to this repository and inherit its access.

## Troubleshooting

**`dco-check` fails on my feature PR.** One or more commits lack a
`Signed-off-by` trailer, or its email differs from the commit author email.
Fix the email in `git config user.email`, then:

```bash
git rebase --signoff origin/main
git push --force-with-lease
```

If your commits were authored with a different email, fix the author too:
`git rebase -r origin/main --exec 'git commit --amend --no-edit --reset-author -s'`.

**Pushing to `upstream-main` is rejected as non-fast-forward.** Someone
committed to `upstream-main`. Find the stray commits with
`git log origin/upstream-main --not upstream/main`. Move them to a feature
branch if they are needed, then ask an admin to reset `upstream-main` to
`upstream/main`. This needs a temporary ruleset bypass.

**Publishing fails with 403 from GHCR.** The org's package settings must
allow members to create packages, and the package's **Manage Actions access**
must give this repository write access.

**A sync conflict touches code you don't own.** Resolve what you can, then ask
the feature's author to review the `sync/<date>` PR before merging it.
