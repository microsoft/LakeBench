# Contributing

See the [README](README.md) for project setup, engine support and usage.
This page describes the pull request review policy.

## Draft pull requests and Copilot review

1. Open a focused draft PR and link its issue. Eligible drafts receive an automatic
   Copilot review through the repository's all-branches ruleset.
2. Address useful findings and keep validation evidence current. The repository
   does not request another review on every push. Authors' personal review-on-push
   settings can independently request additional reviews.
3. When the draft first becomes ready for human review, the first-ready workflow
   requests one additional Copilot review at Balanced effort. It does not request
   more reviews on subsequent draft-to-ready transitions.
4. After substantial later changes, manually re-request Copilot review from the
   PR's **Reviewers** panel. Marking a PR ready is not itself proof that its latest
   code has been reviewed.

Copilot feedback is advisory. Its approvals are disabled and do not count toward
the required approval. Existing CI checks and a qualifying human approval remain
required, subject to the explicitly configured individual maintainer bypass.
Unresolved review threads can block merging: assess each finding, implement it or
explain why it is not applicable, then resolve the discussion.

## Usage and availability

Native automatic reviews normally consume the author's Copilot entitlement unless
organization billing policy overrides that. The workflow's bot-requested review is
billed to the organization. Reviews also use GitHub Actions resources; eligibility,
budgets and organization policies still apply. The workflow does not grant licenses,
enable paid usage, or change billing limits.

GitHub manages the review models and context windows; the workflow selects Balanced
effort, not a particular model. Local Copilot CLI model settings do not control this
service.

## First-ready workflow behavior and recovery

The workflow runs on `pull_request_target: ready_for_review`. It reads PR metadata
and never checks out or executes PR-head code. It preserves human and team reviewer
requests. A bot-authored status comment records its one-time request so rerunning
the workflow cannot silently generate another paid review.

If another Copilot review is pending, the workflow waits up to five minutes before
requesting the additional review. A timeout, licensing denial, API failure or
ambiguous earlier attempt is reported in the workflow logs and status comment.
Inspect those before requesting a review manually. Do not delete the status comment
to retry: it is the duplicate-request guard, not a review discussion.

The PR must still be open and ready when the request is made. Returning to draft
cancels a waiting request; rapid repeated transitions can also leave the first
transition unprocessed. In either case, request a review manually when ready.
PRs whose first ready transition predates installation need a manual re-review.

The first-ready workflow is effective only after it exists on the default branch.
Maintainers should verify it on a real first-ready transition after merging it.
The native ruleset is recorded in
[`.github/rulesets/copilot-code-review.json`](.github/rulesets/copilot-code-review.json);
changing that file alone does not update GitHub settings.

To check the workflow behavior locally, run:

```bash
node --test tests/workflows/copilot-ready-review.test.cjs
actionlint .github/workflows/copilot-ready-review.yml .github/workflows/tests.yml
```
