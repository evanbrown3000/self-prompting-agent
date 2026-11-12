import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

PROMPT_PATH = Path("prompts/current.md")
HAR_PATH = Path("artifacts/agent-provider.har")
ENDPOINTS = [
    "https://social.duoleads.com/a2a",
    "https://localmark.ai/a2a",
]
COMPLETE = {"completed", "complete", "succeeded", "success", "done"}


def state_of(obj):
    if not isinstance(obj, dict):
        return ""
    status = obj.get("status")
    if isinstance(status, dict):
        return str(status.get("state") or status.get("status") or "").lower()
    return str(status or obj.get("state") or "").lower()


def task_id_of(obj):
    if not isinstance(obj, dict):
        return None
    for key in ("taskId", "task_id", "id"):
        value = obj.get(key)
        if isinstance(value, str) and value:
            return value
    for key in ("task", "result"):
        value = obj.get(key)
        if isinstance(value, dict):
            found = task_id_of(value)
            if found:
                return found
    return None


def collect_text(obj):
    found = []
    def walk(value):
        if isinstance(value, str):
            return
        if isinstance(value, list):
            for item in value:
                walk(item)
            return
        if not isinstance(value, dict):
            return
        for key in ("text", "output", "response", "content", "message", "result", "artifacts", "parts"):
            value2 = value.get(key)
            if isinstance(value2, str):
                found.append(value2)
            elif isinstance(value2, (dict, list)):
                walk(value2)
    walk(obj)
    # Preserve order while removing duplicate fragments.
    return "\n".join(dict.fromkeys(x.strip() for x in found if x.strip()))


async def rpc(request, endpoint, method, params, request_id):
    response = await request.post(
        endpoint,
        data={"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        headers={"content-type": "application/json", "accept": "application/json"},
        timeout=90000,
    )
    body = await response.text()
    try:
        parsed = json.loads(body)
    except Exception:
        parsed = {"raw": body[:3000]}
    return response.status, parsed


async def invoke_endpoint(context, endpoint, prompt):
    print(f"PROVIDER_ENDPOINT={endpoint}")
    parsed_url = urlparse(endpoint)
    origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
    page = await context.new_page()
    try:
        try:
            await page.goto(origin, wait_until="domcontentloaded", timeout=20000)
            print(f"BROWSER_OPENED={origin}")
        except Exception as exc:
            print(f"BROWSER_OPEN_ERROR={type(exc).__name__}: {exc}")

        # Probe both common Agent Card locations before sending a turn.
        for card_path in ("/.well-known/agent-card.json", "/.well-known/agent.json"):
            try:
                card = await context.request.get(origin + card_path, timeout=15000)
                print(f"AGENT_CARD {card_path} HTTP {card.status}")
                if card.ok:
                    card_text = (await card.text())[:3000]
                    print(f"AGENT_CARD_BODY={card_text}")
                    break
            except Exception as exc:
                print(f"AGENT_CARD_ERROR {card_path}: {type(exc).__name__}")

        message = {
            "messageId": str(uuid.uuid4()),
            "role": "user",
            "parts": [{"text": prompt}],
        }
        attempts = [
            ("message/send", {"message": message}),
            ("tasks/send", {"id": str(uuid.uuid4()), "sessionId": str(uuid.uuid4()), "message": message}),
        ]
        last_error = None
        for method, params in attempts:
            try:
                status, result = await rpc(context.request, endpoint, method, params, str(uuid.uuid4()))
                print(f"A2A_SEND method={method} http={status} body={json.dumps(result, ensure_ascii=False)[:5000]}")
                if status < 200 or status >= 300 or result.get("error"):
                    last_error = f"{method} HTTP {status}: {result}"
                    continue
                payload = result.get("result", result)
                state = state_of(payload)
                output = collect_text(payload)
                if state in COMPLETE and output:
                    return {"provider": endpoint, "state": state, "output": output, "task_id": task_id_of(payload)}
                task_id = task_id_of(payload)
                if task_id:
                    for attempt in range(45):
                        await asyncio.sleep(2)
                        poll_status, polled = await rpc(
                            context.request, endpoint, "tasks/get", {"id": task_id}, str(uuid.uuid4())
                        )
                        print(f"A2A_POLL attempt={attempt+1} http={poll_status} body={json.dumps(polled, ensure_ascii=False)[:2500]}")
                        if poll_status < 200 or poll_status >= 300 or polled.get("error"):
                            continue
                        polled_payload = polled.get("result", polled)
                        polled_state = state_of(polled_payload)
                        polled_output = collect_text(polled_payload)
                        if polled_state in COMPLETE and polled_output:
                            return {"provider": endpoint, "state": polled_state, "output": polled_output, "task_id": task_id}
                        if polled_state in {"failed", "canceled", "cancelled", "rejected"}:
                            last_error = f"task {task_id} ended in {polled_state}"
                            break
                elif output:
                    # An immediate message response is a completed provider turn.
                    return {"provider": endpoint, "state": "completed", "output": output, "task_id": None}
            except Exception as exc:
                last_error = f"{method}: {type(exc).__name__}: {exc}"
                print(f"A2A_ATTEMPT_ERROR={last_error}")
        print(f"A2A_ENDPOINT_FAILED={last_error or 'no completed response'}")
        return None
    finally:
        await page.close()


async def main():
    if not PROMPT_PATH.exists():
        raise SystemExit("Missing prompts/current.md")
    prompt = PROMPT_PATH.read_text(encoding="utf-8").strip()
    if not prompt:
        raise SystemExit("Prompt is empty")
    HAR_PATH.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(record_har_path=str(HAR_PATH))
        try:
            for endpoint in ENDPOINTS:
                result = await invoke_endpoint(context, endpoint, prompt)
                if result:
                    print("AGENT_TURN_COMPLETE")
                    print("PROVIDER=" + result["provider"])
                    print("TASK_ID=" + str(result["task_id"]))
                    print("STATE=" + result["state"])
                    print("AGENT_RESPONSE_BEGIN")
                    print(result["output"])
                    print("AGENT_RESPONSE_END")
                    return
            print("NO_A2A_AGENT_COMPLETED")
            raise SystemExit(2)
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
