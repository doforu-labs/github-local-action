# Publishing checklist (for maintainers)

[简体中文](publishing.zh-CN.md)

Run through this before flipping a private repository to public. The first two sections are already done; sections three and four can only be done by the repository owner.

## 1. Things already handled in code

| Item | Status |
|---|---|
| License | `LICENSE` (MIT); dependencies are aiohttp (Apache-2.0) and optional paramiko (LGPL-2.1), no conflict |
| Security policy / contributing / code of conduct | `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md` |
| CI | `.github/workflows/test.yml`: Python 3.11/3.13, Node 20/22, Windows frontend tests, e2e, worker type check, PII scan |
| Unified test entry point | `make test` / `scripts/run_tests.py` |
| Dependency locking | `remote-worker/package-lock.json` (reproducible with `npm ci`) |
| Installable | `local-client/pyproject.toml`: `pip install .` provides the `github-local-action` command, and the status page plus `/agent.md` ship with the package |
| Desensitisation | Real email addresses, private IPs, real hostnames, the Cloudflare account id and the D1 resource id are replaced with placeholders; `make pii` catches them again |
| Can a stranger get it running | `remote-worker/schema.sql` now creates the `webhook_secrets` table (automatic webhook configuration used to 500); the `d1 create` database name in the README matches `wrangler.toml` |
| Security boundary | `POST /config` is local-only now (LAN devices could previously change `ws_url` and the token); `tests/unit_local_only.py` locks that down |
| Gate on self-hosted workflows | Every job in `platform-*.yml` is gated on a repository variable: `if: vars.RUN_SELF_HOSTED == 'true'`. Unset means it never runs (forks are off by default), and no organisation or repository name is hardcoded |
| Docs | README is a front page plus `docs/quickstart`, `architecture`, `self-hosted-runner`, `known-limitations`, `lab-notes`, with three real screenshots; every doc exists in English and Chinese |

## 2. Git history: rewritten once already

**Status: done** (2026-11-14). The original history carried the lab machine's address / username / password, an account email, the Cloudflare account id and account subdomain, real hostnames, private repository names, and the organisation name — making it public would have published all of it permanently. It was replaced item by item with `git filter-repo`; all 86 commits survived and the old values inside commit messages were replaced too.

**A complete backup was taken first**: `~/Desktop/github-local-action-backup-20261114.bundle` (a git bundle with the entire pre-rewrite history). To restore, `git clone <that bundle> <directory>`.

Doing the same again later (say, another credential slips into a commit):

```bash
# 1) back up; do not skip this
git bundle create ~/gla-backup-$(date +%Y%m%d).bundle --all

# 2) keep the replacement list outside the repo, one "old==>new" per line.
#    Note: filter-repo does not understand # comments — every line is an expression,
#    and a bare # line becomes "match every hash sign" and rewrites all comments
#    in the repository into a deletion marker (this was hit for real).
printf '%s\n' 'old-address==>10.0.0.21' 'old-user==>tester' > /tmp/redact.txt

# 3) replace both file contents and commit messages
git filter-repo --replace-text /tmp/redact.txt --replace-message /tmp/redact.txt

# 4) verify: this should print nothing
git log -p | grep -nE 'old-address|old-user'
```

Every commit hash changes afterwards, so the remote needs `git push --force`. Confirm there are no other collaborators and no other clones before pushing.

### One thing left: commit authorship

All 86 commits are authored by the same organisation account (its noreply address contains the organisation name). It is visible in `git log`, but it is really "which account will you publish under", not a leak — switching to a personal account is one command:

```bash
printf '%s\n' 'Your Name <123456+you@users.noreply.github.com> old-org-name <old-noreply>' > /tmp/mailmap
git filter-repo --mailmap /tmp/mailmap
```

The cost is that commits no longer link to the original organisation account (which is the point). Rewriting authorship is irreversible, so it is left for you to decide based on the publishing account.

## 3. Publishing (the path actually taken)

The original repository's history carried real credentials. Even after the `git filter-repo` pass, those old objects still exist on GitHub's side, so flipping that repository is a gamble on the rewrite having been perfect. What actually works better: leave the old repository private as a backup and publish a fresh one with the clean history.

```bash
# 1) free up the name — the old repository stays private
gh api -X PATCH repos/<owner>/github-local-action -f name=github-local-action-pre-rewrite

# 2) create the public one and push
gh repo create <owner>/github-local-action --public --description "one line about the project"
git remote set-url origin git@github.com:<owner>/github-local-action.git
git push -u origin main
```

**The push is rejected if you go through gh's OAuth token.** That token has no `workflow` scope, and this repository contains `.github/workflows/*`:

```
! [remote rejected] main -> main (refusing to allow an OAuth App to create or
  update workflow `.github/workflows/platform-check.yml` without `workflow` scope)
```

Either run `gh auth refresh -s workflow`, or push over SSH — SSH keys carry no scopes and it takes one command.

Then verify anonymity (no token, no login):

```bash
curl -o /dev/null -w '%{http_code}\n' https://api.github.com/repos/<owner>/<repo>
curl -o /dev/null -w '%{http_code}\n' https://raw.githubusercontent.com/<owner>/<repo>/main/README.md
curl -sL -o /tmp/t.tar.gz -w '%{http_code} %{size_download}\n' \
  https://codeload.github.com/<owner>/<repo>/tar.gz/refs/heads/main
tar -tzf /tmp/t.tar.gz | head    # screenshots and docs must be inside the tarball
```

The first real CI run will find things no local run could. This repository went red twice on `windows-latest`, both times inside `scripts/run_tests.py` (a cp1252 locale decoding test output, then failing to print Chinese test names): read it with `gh run view <id> --log-failed`, fix, push again. Dependabot also starts the moment the repository is public — security updates are on by default, so a handful of major-version PRs arriving at once is normal, not a problem you introduced.

Flipping the original repository instead is irreversible: history becomes permanently public and is immediately captured by forks. Undoing that means deleting the whole repository.

## 4. Worth enabling after going public

- **Settings → Code security**: Secret scanning + Push protection (so a future accidental token commit is blocked)
- **Settings → General → Features**: Discussions (so questions do not pile up as issues)
- **Settings → Branches**: protect `main`, require CI plus at least one review. This repository itself only blocks force-pushes and deletions: its maintainer token carries no `workflow` scope, so a Dependabot PR touching `.github/workflows/*` cannot be merged through the API at all — it has to be pushed. Add the pull-request requirement once you hold a token with that scope.
- **Settings → Actions → General**: require maintainer approval for workflow runs from fork PRs
- **About**: a one-line description plus topics:
  `github-webhook` `cloudflare-workers` `self-hosted-runner` `sse` `sqlite` `mcp` `local-first`
- **Releases**: tag `v0.1.0` and say plainly "first public version, interfaces will change"
- **Pin one issue**: state the capability boundaries (no live logs during a run, LAN endpoints unauthenticated)

## 5. Every change after going public

```bash
make test && make pii && make typecheck
```

`make pii` is a hard gate: a real email address, private IP, token or real hostname slipping in turns CI red.
