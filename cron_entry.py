import meta_reel_scheduler as scheduler

if __name__ == "__main__":
    scheduler.cloud_sync_from_remote()
    raise SystemExit(scheduler.run_once(live=True))
