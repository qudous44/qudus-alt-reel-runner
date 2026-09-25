# Cloud dispatcher option

The publisher stays in GitHub Actions. This small Render cron job only requests
a workflow run; it does not copy Meta or Cloudinary credentials to Render.

- Repository: `https://github.com/qudous44/qudus-alt-reel-runner`
- Branch: `main`
- Runtime: Python
- Build command: `echo ready`
- Start command: `python dispatch_workflow.py`
- Schedule: `2/5 * * * *` (UTC, every five minutes)
- Environment variable: `GH_DISPATCH_TOKEN`

Use a fine-grained GitHub personal access token restricted to this repository
with **Actions: Read and write**. Render cron has a $1/month minimum charge.
After a scheduled dispatch is observed, leave the GitHub schedule as backup.
The existing GitHub concurrency group and cloud publish state prevent
overlapping publisher runs from posting the same due item.
