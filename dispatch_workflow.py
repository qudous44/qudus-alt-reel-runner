"""Trigger the cloud reel publisher from an external cron service."""
import json
import os
import urllib.error
import urllib.request

WORKFLOW_URL = (
    "https://api.github.com/repos/qudous44/qudus-alt-reel-runner/"
    "actions/workflows/publish.yml/dispatches"
)


def main() -> None:
    token = os.environ["GH_DISPATCH_TOKEN"]
    request = urllib.request.Request(
        WORKFLOW_URL,
        data=json.dumps({"ref": "main"}).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "qudus-alt-reel-dispatcher",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if response.status != 204:
                raise RuntimeError(f"Unexpected dispatch status: {response.status}")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"GitHub dispatch failed: HTTP {exc.code}") from None
    print("Reel publisher dispatch accepted")


if __name__ == "__main__":
    main()
