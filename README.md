# Scheduled Reel Worker

Windows Task Scheduler runs QudusAltCloudDispatchFallback every two minutes while the PC is on and user 123 is signed in. Its versioned hidden VBScript launcher dispatches the GitHub workflow without a Command Prompt window. It records the dispatch exit code in the local app data log; the task is limited to two minutes.

The workflow publishes at most one due reel per run. Cloudinary persists state, and the GitHub concurrency group prevents overlapping publishers. The local QudusAltReels100Daily Python publisher remains disabled. Runtime credentials and publishing state are not stored here.
