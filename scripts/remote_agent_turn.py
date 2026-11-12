import asyncio
import json
import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

PROMPT_PATH = Path("prompts/current.md")
HAR_PATH = Path("artifacts/agent-provider.har")
AGENT_ORIGIN = "https://documents.localmark.ai"
ENDPOINTS = [AGENT_ORIGIN + "/a2a"]
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
        for key in ("text", "output", "response", "content", "message", "result", "artifacts", "parts", "data"):
            value2 = value.get(key)
            if isinstance(value2, str):
                found.append(value2)
            elif isinstance(value2, (dict, list)):
                walk(value2)

    walk(obj)
    return "\n".join(dict.fromkeys(x.strip() for x in found if x.strip()))


async def preflight_provider(context):
    page = await context.new_page()
    try:
        try:
            await page.goto(AGENT_ORIGIN, wait_until="domcontentloaded", timeout=20000)
            print("BROWSER_OPENED=" + AGENT_ORIGIN)
        except Exception as exc:
            print(f"BROWSER_OPEN_ERROR={type(exc).__name__}: {exc}")
        for path in (
            "/.well-known/agent-card.json",
            "/openapi.json",
            "/api/pricing",
            "/api/payment-terms",
        ):
            try:
                response = await context.request.get(AGENT_ORIGIN + path, timeout=15000)
                body = await response.text()
                print(f"PREFLIGHT_GET {path} HTTP {response.status}")
                print("PREFLIGHT_BODY=" + body[:10000])
            except Exception as exc:
                print(f"PREFLIGHT_ERROR {path}: {type(exc).__name__}: {exc}")
    finally:
        await page.close()


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
    request_id = str(uuid.uuid4())
    envelope = {
        "request_id": request_id,
        "allow_model_processing": True,
        "input": {
            "text": prompt,
            "question": (
                "Evaluate the architecture in the supplied text. Identify the strongest existing "
                "implementation to extend, the best free-tier agent/browser route, the durable "
                "cross-provider execution design, and one measurable next production action. "
                "Distinguish evidence from recommendations."
            ),
        },
    }
    message = {
        "messageId": str(uuid.uuid4()),
        "role": "user",
        "parts": [{"kind": "data", "data": envelope}],
    }
    status, result = await rpc(context.request, endpoint, "message/send", {"message": message}, str(uuid.uuid4()))
    print(f"A2A_SEND method=message/send http={status} body={json.dumps(result, ensure_ascii=False)[:10000]}")
    if status == 402:
        print("NO_PAYMENT_AUTHORIZED; free trial unavailable or payment required; stopping without payment")
        return None
    if status < 200 or status >= 300 or result.get("error"):
        print("A2A_SEND_FAILED")
        return None

    payload = result.get("result", result)
    state = state_of(payload)
    output = collect_text(payload)
    task_id = task_id_of(payload)
    if state in COMPLETE and output:
        return {"provider": endpoint, "state": state, "output": output, "task_id": task_id, "request_id": request_id}
    if task_id:
        for attempt in range(45):
            await asyncio.sleep(2)
            poll_status, polled = await rpc(
                context.request, endpoint, "tasks/get", {"id": task_id}, str(uuid.uuid4())
            )
            print(f"A2A_POLL attempt={attempt+1} http={poll_status} body={json.dumps(polled, ensure_ascii=False)[:2500]}")
            if poll_status == 402:
                print("NO_PAYMENT_AUTHORIZED; stopping without payment")
                return None
            if poll_status < 200 or poll_status >= 300 or polled.get("error"):
                continue
            polled_payload = polled.get("result", polled)
            polled_state = state_of(polled_payload)
            polled_output = collect_text(polled_payload)
            if polled_state in COMPLETE and polled_output:
                return {"provider": endpoint, "state": polled_state, "output": polled_output, "task_id": task_id, "request_id": request_id}
            if polled_state in {"failed", "canceled", "cancelled", "rejected"}:
                print("A2A_TASK_TERMINAL_FAILURE=" + polled_state)
                return None
    elif output and len(output.strip()) >= 200:
        return {"provider": endpoint, "state": "completed", "output": output, "task_id": None, "request_id": request_id}
    print("A2A_NO_VERIFIED_TERMINAL_RESULT")
    return None


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
            if os.getenv("PREFLIGHT_ONLY") == "1":
                await preflight_provider(context)
                print("PREFLIGHT_COMPLETE")
                return
            for endpoint in ENDPOINTS:
                result = await invoke_endpoint(context, endpoint, prompt)
                if result:
                    print("AGENT_TURN_COMPLETE")
                    print("PROVIDER=" + result["provider"])
                    print("REQUEST_ID=" + result["request_id"])
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
