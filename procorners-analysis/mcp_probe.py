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

    def call_data(self, name: str, arguments: dict | None = None):
        """ينادي أداة ويستخرج بياناتها الفعلية من result.content[0].text (JSON إن أمكن)."""
        return _extract_data(self.call_tool(name, arguments))


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


def _extract_data(result):
    """يحوّل ناتج tools/call إلى البيانات الفعلية (يفك JSON داخل content[0].text)."""
    if not isinstance(result, dict):
        return result
    sc = result.get("structuredContent")
    if sc:
        return sc
    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and "text" in item:
                txt = item["text"]
                try:
                    return json.loads(txt)
                except (json.JSONDecodeError, TypeError):
                    return txt
    return result


def _as_list(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("posts", "items", "data", "results", "terms", "products", "users", "comments"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        if "_error" in data:
            return []
    return []


def _pid(obj):
    if isinstance(obj, dict):
        for k in ("ID", "id", "post_id", "post_ID"):
            if k in obj and obj[k] is not None:
                return obj[k]
    return None


def _meta1(meta, key):
    if not isinstance(meta, dict):
        return None
    v = meta.get(key)
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _safe(client, name, arguments):
    try:
        return client.call_data(name, arguments)
    except MCPError as exc:
        return {"_error": str(exc)}


def _cell(s, n=60):
    s = "" if s is None else str(s)
    s = s.replace("\n", " ").replace("|", "/").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


SEO_META = {
    "Yoast": ("_yoast_wpseo_title", "_yoast_wpseo_metadesc"),
    "RankMath": ("rank_math_title", "rank_math_description"),
    "AIOSEO": ("_aioseo_title", "_aioseo_description"),
    "SEOPress": ("_seopress_titles_title", "_seopress_titles_desc"),
}


def cmd_snapshot(args) -> int:
    _ensure_out()
    c = Client()
    info = c.handshake()
    si = info.get("serverInfo", {})
    print(f"✓ متصل: {si.get('name','?')} v{si.get('version','?')} — جارٍ جمع اللقطة...")

    ping = _safe(c, "mcp_ping", {})
    post_types = _safe(c, "wp_get_post_types", {})
    counts = {pt: _safe(c, "wp_count_posts", {"post_type": pt})
              for pt in ("product", "post", "page", "shop_order")}
    term_counts = {
        "product_cat": _safe(c, "wp_count_terms", {"taxonomy": "product_cat"}),
        "product_tag": _safe(c, "wp_count_terms", {"taxonomy": "product_tag"}),
    }
    media_count = _safe(c, "wp_count_media", {})
    plugins = _as_list(_safe(c, "wp_list_plugins", {}))
    cats = _as_list(_safe(c, "wp_get_terms", {"taxonomy": "product_cat", "limit": 200}))

    # كل المنتجات بالترقيم
    products = []
    offset, page_size = 0, 100
    while True:
        batch = _as_list(_safe(c, "wp_get_posts",
                               {"post_type": "product", "limit": page_size, "offset": offset}))
        if not batch:
            break
        products.extend(batch)
        if len(batch) < page_size or len(products) >= args.max_products:
            break
        offset += page_size

    # عيّنة موزّعة من المنتجات للقطات تفصيلية
    sample_n = min(args.sample, len(products))
    step = max(1, len(products) // sample_n) if sample_n else 1
    sample_ids = []
    for i in range(0, len(products), step):
        pid = _pid(products[i])
        if pid is not None:
            sample_ids.append(pid)
        if len(sample_ids) >= sample_n:
            break

    snapshots, seo_detected = [], set()
    for pid in sample_ids:
        snap = _safe(c, "wp_get_post_snapshot",
                     {"ID": int(pid), "include": ["meta", "terms", "thumbnail"]})
        snapshots.append({"ID": pid, "snap": snap})
        meta = snap.get("meta") if isinstance(snap, dict) else None
        if isinstance(meta, dict):
            for plugin, (tk, dk) in SEO_META.items():
                if tk in meta or dk in meta:
                    seo_detected.add(plugin)

    raw = {"serverInfo": si, "ping": ping, "post_types": post_types, "counts": counts,
           "term_counts": term_counts, "media_count": media_count, "plugins": plugins,
           "categories": cats, "products": products, "snapshots": snapshots,
           "seo_detected": sorted(seo_detected)}
    with open(os.path.join(OUT_DIR, "snapshot-raw.json"), "w", encoding="utf-8") as fh:
        json.dump(raw, fh, ensure_ascii=False, indent=2)

    # ===== بناء الملخّص المضغوط =====
    L = []
    L.append(f"# لقطة متجر — {si.get('name','')}")
    L.append("")
    L.append(f"- mcp_ping: `{json.dumps(ping, ensure_ascii=False)[:200]}`")
    L.append(f"- عدّادات المنشورات: {json.dumps(counts, ensure_ascii=False)}")
    L.append(f"- عدّاد التصنيفات: {json.dumps(term_counts, ensure_ascii=False)} | الوسائط: {json.dumps(media_count, ensure_ascii=False)}")
    L.append(f"- إضافة السيو المكتشفة: {', '.join(sorted(seo_detected)) or 'غير مؤكّدة (راجع العيّنة)'}")
    L.append("")

    L.append(f"## الإضافات ({len(plugins)})")
    for p in plugins:
        if isinstance(p, dict):
            L.append(f"- {_cell(p.get('Name') or p.get('name'), 50)} `{p.get('Version') or p.get('version','')}`")
    L.append("")

    L.append(f"## تصنيفات المنتجات ({len(cats)})")
    for t in cats:
        if isinstance(t, dict):
            name = t.get("name") or t.get("term_name")
            cnt = t.get("count")
            parent = t.get("parent")
            desc = "✗" if not (t.get("description") or "").strip() else "✓"
            L.append(f"- {_cell(name,40)} — عدد:{cnt} أب:{parent} وصف:{desc}")
    L.append("")

    # ملخّص قائمة المنتجات
    by_status, no_excerpt = {}, 0
    for p in products:
        if not isinstance(p, dict):
            continue
        st = p.get("status") or p.get("post_status") or "?"
        by_status[st] = by_status.get(st, 0) + 1
        exc = p.get("excerpt") or p.get("post_excerpt") or ""
        if not str(exc).strip():
            no_excerpt += 1
    L.append(f"## المنتجات (مسحوب {len(products)})")
    L.append(f"- حسب الحالة: {json.dumps(by_status, ensure_ascii=False)}")
    L.append(f"- بلا excerpt في القائمة: {no_excerpt}")
    L.append("")

    # جدول العيّنة التفصيلية
    L.append(f"## عيّنة تفصيلية ({len(snapshots)} منتج)")
    L.append("ID | العنوان | الحالة | السعر | SKU | المخزون | وصف_قصير | وصف_طويل | #تصنيف | صورة | SEOعنوان | SEOوصف")
    L.append("---|---|---|---|---|---|---|---|---|---|---|---")
    for s in snapshots:
        snap = s["snap"]
        post = snap.get("post", {}) if isinstance(snap, dict) else {}
        meta = snap.get("meta", {}) if isinstance(snap, dict) else {}
        terms = snap.get("terms") if isinstance(snap, dict) else None
        thumb = snap.get("thumbnail") if isinstance(snap, dict) else None
        title = post.get("post_title") or post.get("title")
        status = post.get("post_status") or post.get("status")
        price = _meta1(meta, "_price")
        sku = _meta1(meta, "_sku")
        stock_status = _meta1(meta, "_stock_status")
        short_len = len(str(post.get("post_excerpt") or "").strip())
        long_len = len(str(post.get("post_content") or "").strip())
        if isinstance(terms, dict):
            tl = terms.get("product_cat")
            ncat = len(tl) if isinstance(tl, list) else (len(_as_list(terms)) if not tl else 0)
        else:
            ncat = len(_as_list(terms))
        has_thumb = "✓" if thumb else "✗"
        seo_t = seo_d = "✗"
        if isinstance(meta, dict):
            for _plugin, (tk, dk) in SEO_META.items():
                if _meta1(meta, tk):
                    seo_t = "✓"
                if _meta1(meta, dk):
                    seo_d = "✓"
        L.append(f"{s['ID']} | {_cell(title,40)} | {status} | {price} | {_cell(sku,16)} | "
                 f"{stock_status} | {short_len} | {long_len} | {ncat} | {has_thumb} | {seo_t} | {seo_d}")
    L.append("")
    L.append("> الملف الكامل في out/snapshot-raw.json — الصق هذا الملخّص لـ Claude لبدء التحليل والمسودّات.")

    digest = "\n".join(L)
    with open(os.path.join(OUT_DIR, "digest.md"), "w", encoding="utf-8") as fh:
        fh.write(digest)
    print(f"✓ حُفظت اللقطة: out/snapshot-raw.json + out/digest.md\n")
    print(digest)
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

    p_snap = sub.add_parser("snapshot", help="لقطة كاملة للمتجر + ملخّص مضغوط للتحليل")
    p_snap.add_argument("--max-products", dest="max_products", type=int, default=2000,
                        help="أقصى عدد منتجات تُسحب (افتراضي 2000)")
    p_snap.add_argument("--sample", type=int, default=12,
                        help="عدد المنتجات في العيّنة التفصيلية (افتراضي 12)")

    args = parser.parse_args(argv)
    try:
        if args.command == "discover":
            return cmd_discover(args)
        if args.command == "call":
            return cmd_call(args)
        if args.command == "pull":
            return cmd_pull(args)
        if args.command == "snapshot":
            return cmd_snapshot(args)
    except MCPError as exc:
        print(f"خطأ: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
