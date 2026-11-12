import asyncio
import json
import os
import uuid
from pathlib import Path

from playwright.async_api import async_playwright

PROMPT_PATH = Path("prompts/current.md")
HAR_PATH = Path("artifacts/agent-provider.har")
AGENT_ORIGIN = "https://documents.localmark.ai"
A2A_ENDPOINT = AGENT_ORIGIN + "/a2a"
COMPLETE = {"completed", "complete", "succeeded", "success", "done"}


def state_of(obj):
    if isinstance(obj, dict):
        status = obj.get("status")
        if isinstance(status, dict):
            state = status.get("state") or status.get("status")
            if state:
                return str(state).lower()
        elif status:
            return str(status).lower()
        state = obj.get("state")
        if state:
            return str(state).lower()
        for value in obj.values():
            found = state_of(value)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = state_of(value)
            if found:
                return found
    return ""


def task_id_of(obj):
    if isinstance(obj, dict):
        for key in ("taskId", "task_id", "id"):
            value = obj.get(key)
            if isinstance(value, str) and value:
                return value
        for value in obj.values():
            found = task_id_of(value)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = task_id_of(value)
            if found:
                return found
    return None


def find_analysis(obj):
    if isinstance(obj, dict):
        if isinstance(obj.get("summary"), str) and isinstance(obj.get("output"), dict):
            return obj
        for value in obj.values():
            found = find_analysis(value)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_analysis(value)
            if found:
                return found
    return None


def collect_text(obj):
    found = []
    keys = {
        "text", "output", "response", "content", "message", "result",
        "artifacts", "parts", "data", "summary", "answer", "quotes",
        "findings", "detail", "recommendation"
    }

    def walk(value):
        if isinstance(value, str):
            return
        if isinstance(value, list):
            for item in value:
                walk(item)
            return
        if not isinstance(value, dict):
            return
        for key, nested in value.items():
            if key in keys:
                if isinstance(nested, str):
                    found.append(nested)
                elif isinstance(nested, (dict, list)):
                    walk(nested)

    walk(obj)
    return "\n".join(dict.fromkeys(x.strip() for x in found if x.strip()))


def trial_is_available(pricing):
    if not isinstance(pricing, dict):
        return False
    trial = pricing.get("pricing", {}).get("trial") if isinstance(pricing.get("pricing"), dict) else None
    return isinstance(trial, dict) and trial.get("available") is True


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


async def preflight_provider(context):
    page = await context.new_page()
    try:
        try:
            await page.goto(AGENT_ORIGIN, wait_until="domcontentloaded", timeout=20000)
            print("BROWSER_OPENED=" + AGENT_ORIGIN)
        except Exception as exc:
            print(f"BROWSER_OPEN_ERROR={type(exc).__name__}: {exc}")
        card_response = await context.request.get(AGENT_ORIGIN + "/.well-known/agent-card.json", timeout=15000)
        card_text = await card_response.text()
        print(f"AGENT_CARD_HTTP={card_response.status}")
        print("AGENT_CARD=" + card_text[:10000])
        pricing_response = await context.request.get(AGENT_ORIGIN + "/api/pricing", timeout=15000)
        pricing_text = await pricing_response.text()
        print(f"PRICING_HTTP={pricing_response.status}")
        print("PRICING=" + pricing_text[:10000])
        try:
            pricing = json.loads(pricing_text)
        except Exception:
            pricing = {}
        return card_response.status, pricing_response.status, pricing
    finally:
        await page.close()


async def invoke_agent(context, prompt, request_id):
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
    rpc_id = str(uuid.uuid4())
    status, result = await rpc(context.request, A2A_ENDPOINT, "message/send", {"message": message}, rpc_id)
    print(f"A2A_SEND_HTTP={status}")
    print("A2A_RESPONSE=" + json.dumps(result, ensure_ascii=False)[:16000])
    if status == 402:
        print("NO_PAYMENT_AUTHORIZED; free trial unavailable or payment required; stopping without payment")
        return None
    if status < 200 or status >= 300 or result.get("error"):
        print("A2A_SEND_FAILED")
        return None

    payload = result.get("result", result)
    analysis = find_analysis(payload)
    state = state_of(payload)
    task_id = task_id_of(payload)
    if analysis and (not state or state in COMPLETE):
        billing = analysis.get("billing") if isinstance(analysis.get("billing"), dict) else {}
        print("BILLING=" + json.dumps(billing, ensure_ascii=False))
        if billing.get("charged") is True:
            print("UNEXPECTED_CHARGE_REPORTED; no further calls will be made")
            return None
        return {
            "request_id": request_id,
            "task_id": task_id,
            "state": state or "completed",
            "analysis": analysis,
            "billing": billing,
        }

    if task_id:
        for attempt in range(45):
            await asyncio.sleep(2)
            poll_status, polled = await rpc(
                context.request, A2A_ENDPOINT, "tasks/get", {"id": task_id}, str(uuid.uuid4())
            )
            print(f"A2A_POLL_{attempt+1}_HTTP={poll_status}")
            print("A2A_POLL_BODY=" + json.dumps(polled, ensure_ascii=False)[:5000])
            if poll_status == 402:
                print("NO_PAYMENT_AUTHORIZED; stopping without payment")
                return None
            if poll_status < 200 or poll_status >= 300 or polled.get("error"):
                continue
            polled_payload = polled.get("result", polled)
            analysis = find_analysis(polled_payload)
            state = state_of(polled_payload)
            if analysis and state in COMPLETE:
                billing = analysis.get("billing") if isinstance(analysis.get("billing"), dict) else {}
                print("BILLING=" + json.dumps(billing, ensure_ascii=False))
                if billing.get("charged") is True:
                    print("UNEXPECTED_CHARGE_REPORTED; no further calls will be made")
                    return None
                return {
                    "request_id": request_id,
                    "task_id": task_id,
                    "state": state,
                    "analysis": analysis,
                    "billing": billing,
                }
            if state in {"failed", "canceled", "cancelled", "rejected"}:
                print("A2A_TASK_TERMINAL_FAILURE=" + state)
                return None
    print("A2A_NO_VERIFIED_STRUCTURED_RESULT")
    print("RAW_TEXT=" + collect_text(payload)[:8000])
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
            card_status, pricing_status, pricing = await preflight_provider(context)
            if card_status != 200 or pricing_status != 200:
                raise SystemExit("Provider preflight failed; no AI call attempted")
            if not trial_is_available(pricing):
                print("FREE_TRIAL_NOT_VERIFIED; no AI call and no payment")
                return
            if os.getenv("PREFLIGHT_ONLY") == "1":
                print("PREFLIGHT_COMPLETE_FREE_TRIAL_AVAILABLE")
                return
            request_id = str(uuid.uuid4())
            result = await invoke_agent(context, prompt, request_id)
            if not result:
                raise SystemExit(2)
            print("AGENT_TURN_COMPLETE")
            print("PROVIDER=" + A2A_ENDPOINT)
            print("REQUEST_ID=" + result["request_id"])
            print("TASK_ID=" + str(result["task_id"]))
            print("STATE=" + result["state"])
            print("BILLING=" + json.dumps(result["billing"], ensure_ascii=False))
            print("AGENT_RESPONSE_BEGIN")
            print(json.dumps(result["analysis"], ensure_ascii=False, indent=2))
            print("AGENT_RESPONSE_END")
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
