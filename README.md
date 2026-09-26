# Scheduled Reel Worker

Code-only cloud runner. Runtime credentials and publishing state are not stored in this repository.

The GitHub workflow publishes one due item, waits for the next due time, then
requests its own next run with the repository's GITHUB_TOKEN. An hourly GitHub
schedule restarts the chain if a run fails. No PC task or paid cron service is
required. The workflow's concurrency group keeps only one publisher active.
