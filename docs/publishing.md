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

## 3. Flipping repository visibility

1. **Check the account's visibility**: if anonymous access to `api.github.com/orgs/<org>` and `api.github.com/repos/<account>/<repo>` returns 404, it is not public. Whether a repository under a private organisation can be converted directly to public varies by plan — try **Settings → General → Danger Zone → Change visibility** once; a refusal comes with a clear error.
2. **Prefer a copy first** over flipping the original:
   ```bash
   git push --mirror git@github.com:<your-account>/github-local-action-public.git
   ```
   Run the README, issue templates and CI on the copy, watch it for a couple of days, then decide about the main repository.
3. Flipping is irreversible: history becomes permanently public and is immediately captured by forks. Undoing it means deleting the whole repository.

## 4. Worth enabling after going public

- **Settings → Code security**: Secret scanning + Push protection (so a future accidental token commit is blocked)
- **Settings → General → Features**: Discussions (so questions do not pile up as issues)
- **Settings → Branches**: protect `main`, require CI plus at least one review
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
