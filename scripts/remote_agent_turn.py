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
PUBLIC_SOURCES = [
    ("DeepWiki OmniRoute", "https://deepwiki.com/evanbrown3000/OmniRoute-evan", 2600),
    ("OmniRoute Agent Protocols", "https://raw.githubusercontent.com/evanbrown3000/OmniRoute-evan/release/v3.8.54/docs/frameworks/AGENT_PROTOCOLS_GUIDE.md", 2800),
    ("OmniRoute A2A Server", "https://raw.githubusercontent.com/evanbrown3000/OmniRoute-evan/release/v3.8.54/docs/frameworks/A2A-SERVER.md", 2200),
    ("OmniRoute Cloud Agents", "https://raw.githubusercontent.com/evanbrown3000/OmniRoute-evan/release/v3.8.54/docs/frameworks/CLOUD_AGENT.md", 2800),
    ("A2A Protocol", "https://raw.githubusercontent.com/a2aproject/A2A/main/README.md", 2400),
    ("OpenHands SDK", "https://raw.githubusercontent.com/OpenHands/software-agent-sdk/main/README.md", 2400),
    ("Graphiti", "https://raw.githubusercontent.com/getzep/graphiti/main/README.md", 2200),
    ("Kilo Code", "https://raw.githubusercontent.com/Kilo-Org/kilocode/main/README.md", 1800),
    ("Cloudflare Browser Run", "https://developers.cloudflare.com/browser-run/", 2000),
    ("OpenHands Cloud pricing", "https://www.openhands.dev/pricing/", 1600),
    ("Agno pricing", "https://www.agno.com/pricing", 1600),
    ("Dify Cloud pricing", "https://dify.ai/pricing/dify-cloud", 1600),
    ("LangSmith pricing", "https://www.langchain.com/pricing", 1600),
]


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

def find_billing(obj):
    if isinstance(obj, dict):
        billing = obj.get("billing")
        if isinstance(billing, dict):
            return billing
        for value in obj.values():
            found = find_billing(value)
            if found:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_billing(value)
            if found:
                return found
    return {}


def substantive_analysis(obj):
    analysis = find_analysis(obj)
    text = collect_text(obj).strip()
    if analysis and (len(text) >= 200 or len(str(analysis.get("summary", "")).strip()) >= 200):
        return analysis
    if len(text) >= 200:
        return {"summary": text[:1200], "output": {"text": text}}
    return None



def trial_info(pricing):
    if not isinstance(pricing, dict):
        return None
    candidates = [pricing.get("trial")]
    nested = pricing.get("pricing")
    if isinstance(nested, dict):
        candidates.append(nested.get("trial"))
    for trial in candidates:
        if not isinstance(trial, dict) or trial.get("available") is not True:
            continue
        scope = trial.get("scope")
        window_days = trial.get("window_days")
        if (
            isinstance(scope, str)
            and scope.strip()
            and isinstance(window_days, (int, float))
            and window_days > 0
        ):
            return {"scope": scope, "window_days": window_days}
    return None


def trial_is_available(pricing):
    return trial_info(pricing) is not None


def write_result(value):
    HAR_PATH.parent.mkdir(parents=True, exist_ok=True)
    (HAR_PATH.parent / "agent-result.json").write_text(
        json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
    )


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
        openapi_response = await context.request.get(AGENT_ORIGIN + "/openapi.json", timeout=15000)
        openapi_text = await openapi_response.text()
        print(f"OPENAPI_HTTP={openapi_response.status}")
        try:
            openapi = json.loads(openapi_text)
            print("OPENAPI_PATHS=" + json.dumps(list(openapi.get("paths", {}).keys())[:80]))
        except Exception:
            openapi = {}
            print("OPENAPI_BODY=" + openapi_text[:3000])
        return card_response.status, pricing_response.status, pricing, openapi_response.status, openapi
    finally:
        await page.close()


async def fetch_public_sources(context):
    page = await context.new_page()
    sections = []
    successful = []
    try:
        for label, url, limit in PUBLIC_SOURCES:
            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=14000)
                status = response.status if response else 0
                if status < 200 or status >= 300:
                    print(f"PUBLIC_SOURCE_HTTP={label} status={status}")
                    continue
                try:
                    body = await page.locator("body").inner_text(timeout=9000)
                except Exception:
                    body = ""
                body = "\\n".join(line.strip() for line in body.splitlines() if line.strip())
                if len(body) < 160:
                    print(f"PUBLIC_SOURCE_EMPTY={label} status={status}")
                    continue
                sections.append(f"### {label}\\nURL: {url}\\nHTTP: {status}\\n{body[:limit]}")
                successful.append(label)
                print(f"PUBLIC_SOURCE_OK={label} status={status} chars={len(body)}")
            except Exception as exc:
                print(f"PUBLIC_SOURCE_ERROR={label} {type(exc).__name__}: {exc}")
    finally:
        await page.close()
    if "DeepWiki OmniRoute" not in successful:
        print("DEEPWIKI_NOT_AVAILABLE; direct GitHub source pages retained")
    required = {"OmniRoute Agent Protocols", "OmniRoute Cloud Agents", "A2A Protocol"}
    missing = sorted(required - set(successful))
    if missing:
        print("PUBLIC_SOURCE_BUNDLE_INCOMPLETE=" + ",".join(missing))
    return "\\n\\n".join(sections), successful, missing


async def invoke_agent(context, prompt, request_id):
    envelope = {
        "request_id": request_id,
        "allow_model_processing": True,
        "input": {
            "text": prompt,
            "question": (
                "Evaluate the supplied public source excerpts. You cannot browse: do not claim "
                "to have visited a URL unless its extracted page text is present. Identify the "
                "strongest existing implementation to extend, the best free-tier agent/browser "
                "route, the durable cross-provider execution design, and one measurable next "
                "production action. Distinguish direct evidence from recommendations."
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
    analysis = substantive_analysis(payload)
    state = state_of(payload)
    task_id = task_id_of(payload)
    if task_id and state in COMPLETE and analysis:
        billing = find_billing(payload)
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

    if task_id and state in {"failed", "canceled", "cancelled", "rejected"}:
        print("A2A_TASK_TERMINAL_FAILURE=" + state)
        return None
    if not task_id:
        print("A2A_NO_ORIGINAL_TASK_ID; acknowledgement is not completion")
        return None

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
        analysis = substantive_analysis(polled_payload)
        state = state_of(polled_payload)
        if analysis and state in COMPLETE:
            billing = find_billing(polled_payload)
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
        if state in COMPLETE:
            print("A2A_TERMINAL_WITHOUT_SUBSTANTIVE_OUTPUT")
            return None
        if state in {"failed", "canceled", "cancelled", "rejected"}:
            print("A2A_TASK_TERMINAL_FAILURE=" + state)
            return None
    print("A2A_NO_VERIFIED_TERMINAL_OUTPUT")
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
            card_status, pricing_status, pricing, openapi_status, openapi = await preflight_provider(context)
            if card_status != 200 or pricing_status != 200:
                write_result({
                    "state": "preflight_failed",
                    "agent_submitted": False,
                    "agent_card_http": card_status,
                    "pricing_http": pricing_status,
                    "openapi_http": openapi_status,
                })
                raise SystemExit("Provider preflight failed; no AI call attempted")

            source_bundle, source_labels, missing_sources = await fetch_public_sources(context)
            if missing_sources:
                write_result({
                    "state": "source_preflight_failed",
                    "agent_submitted": False,
                    "source_labels": source_labels,
                    "missing_sources": missing_sources,
                })
                raise SystemExit("Required public source evidence unavailable; no AI call attempted")

            trial = trial_info(pricing)
            if not trial:
                print("FREE_TRIAL_NOT_VERIFIED; no AI call and no payment")
                write_result({
                    "state": "free_trial_unavailable",
                    "agent_submitted": False,
                    "agent_card_http": card_status,
                    "pricing_http": pricing_status,
                    "openapi_http": openapi_status,
                })
                raise SystemExit(3)
            print(
                "FREE_TRIAL_VERIFIED="
                + json.dumps(trial, ensure_ascii=False)
                + "; no payment signature or paid fallback will be used"
            )

            if os.getenv("PREFLIGHT_ONLY") == "1":
                print("PREFLIGHT_COMPLETE_FREE_TRIAL_AVAILABLE")
                return

            prompt_with_sources = (
                prompt
                + "\\n\\n## Browser-collected public source excerpts\\n"
                + "The LocalMark agent is text-only. The remote Playwright runner fetched the "
                + "following public pages and included their extracted text. A URL without an "
                + "excerpt is not evidence; do not infer page contents or claim it was reviewed. "
                + "Use the HTTP status and source URL attached to each excerpt.\\n\\n"
                + source_bundle
            )
            request_id = str(uuid.uuid4())
            result = await invoke_agent(context, prompt_with_sources, request_id)
            if not result:
                write_result({
                    "state": "unverified",
                    "request_id": request_id,
                    "agent_submitted": True,
                })
                raise SystemExit(2)
            result["agent_submitted"] = True
            result["provider"] = A2A_ENDPOINT
            result["source_labels"] = source_labels
            result["openapi_http"] = openapi_status
            write_result(result)
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
