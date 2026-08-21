# AGENTS.md

## Isolated Git Workflow and Cleanup

- Before changing files, fetch the latest remote default branch and create a new task-specific branch in a disposable clone or isolated `git worktree`. Never make task changes in a shared checkout or directly on `main` or `master`.
- Use a fresh branch, worktree, and directory for every task. Do not reuse a prior task's branch or checkout.
- Keep the task checkout isolated from unrelated repositories and user work. Preserve all pre-existing changes.
- After the work is safely committed and pushed, the pull request is opened or merged as required, and validation results are recorded, remove the disposable local checkout to avoid consuming disk space.
- For a disposable clone, verify `git status --short` is clean and all required commits exist on the remote, then delete only that exact clone directory. For a worktree, run `git worktree remove <exact-path>` from the owning repository and then `git worktree prune`.
- Delete the local task branch only after confirming it is merged or no longer needed and no unpushed commits remain.
- Never delete a shared or canonical clone, the current working directory, an unverified path, or a checkout containing uncommitted, untracked, unpushed, or unrelated work. If cleanup cannot be proven safe, stop and report the exact path and blocker instead of deleting it.
