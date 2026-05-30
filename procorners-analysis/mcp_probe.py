#!/usr/bin/env python3
"""
mcp_probe.py — أداة اتصال وتحليل لخادم MCP الخاص بـ «ركن التسوق» (procorners.com).

عميل MCP خفيف (مكتبة Python القياسية فقط) يتحدّث Streamable HTTP / JSON-RPC 2.0،
ويتعامل مع ردود SSE وجلسة Mcp-Session-Id والترقيم.

الإعداد عبر متغيّرات البيئة (لا أسرار في الكود):
    PROCORNERS_MCP_URL    (اختياري) الافتراضي: https://procorners.com/wp-json/mcp/v1/http
    PROCORNERS_MCP_TOKEN  (مطلوب)   التوكن السرّي للوصول (Bearer)

الأوامر الفرعية:
    discover            مصافحة + tools/list، يحفظ out/tools.json ويطبع ملخّصًا مصنّفًا (قراءة/كتابة)
    call <tool> [--args JSON]   ينادي أداة واحدة ويطبع النتيجة (مع حفظ اختياري)
    pull [--limit N]    سحب أفضل-جهد لأدوات القراءة الشائعة (منتجات/مقالات/صفحات/طلبات...) إلى out/

أمثلة:
    export PROCORNERS_MCP_TOKEN=********
    python3 mcp_probe.py discover
    python3 mcp_probe.py call wp_get_posts --args '{"per_page":5}'
    python3 mcp_probe.py pull --limit 50
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "https://procorners.com/wp-json/mcp/v1/http"
PROTOCOL_VERSION = "2025-06-18"
CLIENT_INFO = {"name": "procorners-probe", "version": "1.0"}

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
SESSION_FILE = os.path.join(OUT_DIR, ".session")

# كلمات تدل على أدوات "كتابة" (تعديل على المتجر) — للتصنيف وتمييز الخطورة.
WRITE_HINTS = (
    "create", "update", "delete", "edit", "set", "write", "add", "remove",
    "publish", "upsert", "modify", "insert", "save", "patch", "put", "post_",
)
# أدوات قراءة شائعة نحاول سحبها في pull (تطابق جزئي بالاسم).
READ_TARGETS = (
    "product", "products", "post", "posts", "page", "pages", "order", "orders",
    "category", "categories", "user", "users", "media", "setting", "settings",
    "term", "comment", "comments",
)


class MCPError(RuntimeError):
    pass


def _ensure_out() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def _token() -> str:
    tok = os.environ.get("PROCORNERS_MCP_TOKEN", "").strip()
    if not tok:
        raise MCPError(
            "متغيّر البيئة PROCORNERS_MCP_TOKEN غير مضبوط.\n"
            "اضبطه أولًا، مثال: export PROCORNERS_MCP_TOKEN=********"
        )
    return tok


def _url() -> str:
    return os.environ.get("PROCORNERS_MCP_URL", DEFAULT_URL).strip() or DEFAULT_URL


def _read_session() -> str | None:
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as fh:
            s = fh.read().strip()
            return s or None
    except OSError:
        return None


def _write_session(sid: str | None) -> None:
    if not sid:
        return
    _ensure_out()
    with open(SESSION_FILE, "w", encoding="utf-8") as fh:
        fh.write(sid)


def _parse_body(content_type: str, body: bytes) -> dict | None:
    """يرجّع أول كائن JSON-RPC يحمل result/error من ردّ JSON أو SSE."""
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    ct = (content_type or "").lower()
    if "text/event-stream" in ct or text.startswith("event:") or "\ndata:" in text or text.startswith("data:"):
        objs = []
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                payload = line[len("data:"):].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    objs.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue
        for obj in objs:
            if isinstance(obj, dict) and ("result" in obj or "error" in obj):
                return obj
        return objs[-1] if objs else None
    # JSON عادي
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise MCPError(f"تعذّر تحليل ردّ الخادم كـ JSON: {exc}\nأول 300 حرف:\n{text[:300]}")


def _post(payload: dict, *, session_id: str | None, is_notification: bool = False):
    """POST واحد إلى نقطة MCP. يرجّع (json_obj_or_None, response_headers)."""
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": PROTOCOL_VERSION,
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    req = urllib.request.Request(_url(), data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            ct = resp_headers.get("content-type", "")
            if is_notification:
                return None, resp_headers
            return _parse_body(ct, raw), resp_headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        deny = exc.headers.get("x-deny-reason") if exc.headers else None
        if exc.code == 403 and deny == "host_not_allowed":
            raise MCPError(
                "محجوب بسياسة شبكة البيئة (host_not_allowed).\n"
                "فعّل السماح لنطاق procorners.com في إعدادات البيئة — راجع SETUP.md (المرحلة 0)."
            )
        raise MCPError(f"HTTP {exc.code}: {body[:300]}")
    except urllib.error.URLError as exc:
        raise MCPError(f"تعذّر الاتصال: {exc.reason}")


class Client:
    def __init__(self) -> None:
        self.session_id: str | None = _read_session()
        self._req_id = 0

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _rpc(self, method: str, params: dict | None = None) -> dict:
        payload = {"jsonrpc": "2.0", "id": self._next_id(), "method": method}
        if params is not None:
            payload["params"] = params
        obj, headers = _post(payload, session_id=self.session_id)
        sid = headers.get("mcp-session-id")
        if sid and sid != self.session_id:
            self.session_id = sid
            _write_session(sid)
        if obj is None:
            raise MCPError(f"ردّ فارغ للطلب {method}")
        if isinstance(obj, dict) and obj.get("error"):
            raise MCPError(f"خطأ من الخادم في {method}: {json.dumps(obj['error'], ensure_ascii=False)}")
        return obj.get("result", {}) if isinstance(obj, dict) else {}

    def _notify(self, method: str, params: dict | None = None) -> None:
        payload = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        _post(payload, session_id=self.session_id, is_notification=True)

    def handshake(self) -> dict:
        result = self._rpc(
            "initialize",
            {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}, "clientInfo": CLIENT_INFO},
        )
        # إشعار جاهزية (لا ردّ متوقّع)
        try:
            self._notify("notifications/initialized")
        except MCPError:
            pass
        return result

    def list_tools(self) -> list[dict]:
        tools: list[dict] = []
        cursor: str | None = None
        while True:
            params = {"cursor": cursor} if cursor else {}
            result = self._rpc("tools/list", params)
            tools.extend(result.get("tools", []) or [])
            cursor = result.get("nextCursor")
            if not cursor:
                break
        return tools

    def call_tool(self, name: str, arguments: dict | None = None) -> dict:
        return self._rpc("tools/call", {"name": name, "arguments": arguments or {}})


def _classify(tool: dict) -> str:
    blob = f"{tool.get('name','')} {tool.get('description','')}".lower()
    return "write" if any(h in blob for h in WRITE_HINTS) else "read"


def cmd_discover(_args) -> int:
    _ensure_out()
    client = Client()
    info = client.handshake()
    si = info.get("serverInfo", {})
    print(f"✓ متصل: {si.get('name','?')} v{si.get('version','?')} "
          f"(protocol {info.get('protocolVersion','?')})")

    tools = client.list_tools()
    with open(os.path.join(OUT_DIR, "tools.json"), "w", encoding="utf-8") as fh:
        json.dump(tools, fh, ensure_ascii=False, indent=2)

    reads = [t for t in tools if _classify(t) == "read"]
    writes = [t for t in tools if _classify(t) == "write"]

    lines = [f"# أدوات خادم MCP — {si.get('name','')}", "",
             f"الإجمالي: **{len(tools)}** أداة — قراءة: {len(reads)} · كتابة (تعدّل المتجر): {len(writes)}", ""]
    for title, group in (("## أدوات القراءة (آمنة)", reads),
                         ("## أدوات الكتابة (تُعدّل المتجر — تتطلب موافقة)", writes)):
        lines.append(title)
        for t in sorted(group, key=lambda x: x.get("name", "")):
            desc = (t.get("description", "") or "").strip().replace("\n", " ")
            if len(desc) > 140:
                desc = desc[:137] + "..."
            lines.append(f"- `{t.get('name','')}` — {desc}")
        lines.append("")
    summary = "\n".join(lines)
    with open(os.path.join(OUT_DIR, "tools-summary.md"), "w", encoding="utf-8") as fh:
        fh.write(summary)

    print(f"✓ {len(tools)} أداة — حُفظت في out/tools.json")
    print(f"  قراءة: {len(reads)} · كتابة: {len(writes)} — الملخّص في out/tools-summary.md")
    print()
    print(summary)
    return 0


def cmd_call(args) -> int:
    _ensure_out()
    arguments = {}
    if args.args:
        arguments = json.loads(args.args)
    client = Client()
    client.handshake()
    result = client.call_tool(args.tool, arguments)
    out = json.dumps(result, ensure_ascii=False, indent=2)
    print(out)
    if args.save:
        path = os.path.join(OUT_DIR, f"{args.tool}.json")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(out)
        print(f"\n✓ حُفظت في {path}", file=sys.stderr)
    return 0


def cmd_pull(args) -> int:
    _ensure_out()
    client = Client()
    client.handshake()
    tools = client.list_tools()
    read_tools = [t for t in tools if _classify(t) == "read"]

    # رشّح أدوات القراءة التي تطابق أهدافًا شائعة (منتجات/مقالات/طلبات...)
    targets = []
    for t in read_tools:
        name = t.get("name", "").lower()
        if any(k in name for k in READ_TARGETS):
            targets.append(t)

    if not targets:
        print("لم أجد أدوات قراءة شائعة للسحب التلقائي. راجع out/tools.json واسحب يدويًا عبر الأمر call.")
        return 0

    manifest = {}
    for t in targets:
        name = t.get("name", "")
        # حاول تمرير حدّ صفحة شائع إن كان مدعومًا في schema
        schema_props = ((t.get("inputSchema") or {}).get("properties") or {})
        arguments = {}
        for key in ("per_page", "perPage", "limit", "count", "number"):
            if key in schema_props:
                arguments[key] = args.limit
                break
        try:
            result = client.call_tool(name, arguments)
            path = os.path.join(OUT_DIR, f"pull-{name}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(result, fh, ensure_ascii=False, indent=2)
            manifest[name] = {"ok": True, "args": arguments, "file": os.path.basename(path)}
            print(f"✓ {name} -> {os.path.basename(path)}")
        except MCPError as exc:
            manifest[name] = {"ok": False, "error": str(exc)}
            print(f"✗ {name}: {exc}")

    with open(os.path.join(OUT_DIR, "pull-manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)
    print(f"\n✓ انتهى السحب — البيان في out/pull-manifest.json")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="عميل/مُحلّل MCP لموقع procorners.com")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("discover", help="مصافحة + سرد الأدوات وتصنيفها")

    p_call = sub.add_parser("call", help="نداء أداة واحدة")
    p_call.add_argument("tool", help="اسم الأداة")
    p_call.add_argument("--args", help="وسائط JSON للأداة", default=None)
    p_call.add_argument("--save", action="store_true", help="حفظ النتيجة في out/")

    p_pull = sub.add_parser("pull", help="سحب أفضل-جهد لأدوات القراءة الشائعة")
    p_pull.add_argument("--limit", type=int, default=50, help="حدّ العناصر لكل أداة (افتراضي 50)")

    args = parser.parse_args(argv)
    try:
        if args.command == "discover":
            return cmd_discover(args)
        if args.command == "call":
            return cmd_call(args)
        if args.command == "pull":
            return cmd_pull(args)
    except MCPError as exc:
        print(f"خطأ: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
