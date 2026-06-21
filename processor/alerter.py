import os

import httpx

def _webhook_url() -> str:
    return os.getenv("SLACK_WEBHOOK_URL", "")

_EMOJI = {"HIGH": ":red_circle:", "MEDIUM": ":large_yellow_circle:", "LOW": ":large_green_circle:"}


async def send_slack(event: dict) -> None:
    webhook_url = _webhook_url()
    if not webhook_url:
        print("[alert] SLACK_WEBHOOK_URL not set, skipping")
        return

    emoji = _EMOJI.get(event["severity"], ":white_circle:")
    text = (
        f"{emoji} *{event['severity']} — {event['anomalyType']}*\n"
        f"Chamber: `{event['chamberId']}` | Equipment: `{event['equipmentId']}`\n"
        f"Sensor: `{event['sensorType']}` | Value: `{event['value']}`\n"
        f"Reason: {event['reason']}\n"
        f"Wafer: `{event.get('waferId', '-')}` | Lot: `{event.get('lotId', '-')}`"
    )

    async with httpx.AsyncClient(timeout=5) as client:
        try:
            resp = await client.post(webhook_url, json={"text": text})
            if resp.status_code != 200:
                print(f"[alert] slack {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"[alert] slack error: {e}")