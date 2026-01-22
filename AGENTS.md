# Agent Requirements

All agents must follow these rules:

1) Fully test your changes before submitting a PR (run the full suite or all relevant tests).
2) PR titles must be descriptive and follow Conventional Commits-style prefixes:
   - Common: `feat:`, `fix:`, `chore:`, `refactor:`, `docs:`, `test:`, `perf:`
   - Support titles: `fix(docs):`, `fix(benchmarks):`, `fix(cicd):`
3) If the branch you're assigned to work on is from a remote (ie origin/master or upstream/awesome-feature) you must ensure you fetch and pull from the remote before you begin your work.
4) Use one of `paths` or `paths-ignore` in every workflow file to make sure workflows only run when required.
5) Maximize the use of caching in GitHub workflow files to minimize run duration.
6) Commit messages must follow the same Conventional Commits-style prefixes and include a short functional description plus a user-facing value proposition.

Reference: https://www.conventionalcommits.org/en/v1.0.0/
