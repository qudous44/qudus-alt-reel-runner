import os, requests

token = os.environ["META_SYSTEM_USER_TOKEN"]
items = [
    ("IG_Q00016", "18109205201334771"),
    ("FB_Q00016", "4653519848308815"),
    ("IG_Q00019", "18121162514512428"),
    ("FB_Q00019", "1793977221800651"),
]

failed = False
for label, media_id in items:
    r = requests.delete(
        f"https://graph.facebook.com/v24.0/{media_id}",
        params={"access_token": token},
        timeout=30,
    )
    try:
        payload = r.json()
    except Exception:
        payload = {"text": r.text[:300]}
    print(label, r.status_code, payload)
    if r.status_code >= 400:
        failed = True

raise SystemExit(1 if failed else 0)
