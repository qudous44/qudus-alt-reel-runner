# Scheduled Reel Worker

Windows Task Scheduler runs QudusAltCloudDispatchFallback every five minutes while the PC is on and user 123 is signed in. Its hidden VBScript launcher dispatches the GitHub workflow without a Command Prompt window.

The workflow publishes at most one due reel per run. Cloudinary persists state, and the GitHub concurrency group prevents overlapping publishers. The local QudusAltReels100Daily Python publisher remains disabled. Runtime credentials and publishing state are not stored here.
