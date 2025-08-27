import asyncio
import datetime
import sqlite3
import time
import os
import argparse
from typing import Dict, Any, List, Tuple, Optional
from contextlib import asynccontextmanager

from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ── DB 設定（環境変数で上書き可） ──────────────────────────────
DB_PATH = os.getenv("MARKET_DB_PATH", os.path.abspath("./market_data.db"))

# ── polite 設定 ────────────────────────────────────────────
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_TARGET_QPS = 0.7
DEFAULT_BATCH_COMMIT = 100
NAV_TIMEOUT_MS = 25000
RETRIES = 3

# ── 必須セレクタ（ページ描画完了の目安：サンプル用） ──────────────
SEL_READY = "xpath=//*[@id='metrics-root']"

# ── 取得 XPaths（サンプルの一般的な構造を想定／任意で調整） ───────
# 例: dl > dd > span[2] のような共通パターンに寄せたダミー
XPATHS: Dict[str, str] = {
    "rating":      "string(//*[@id='metrics-root']//dl[1]/dd/span[2])",
    "sales":       "string(//*[@id='metrics-root']//dl[2]/dd/span[2])",
    "profit":      "string(//*[@id='metrics-root']//dl[3]/dd/span[2])",
    "scale":       "string(//*[@id='metrics-root']//dl[4]/dd/span[2])",
    "cheap":       "string(//*[@id='metrics-root']//dl[5]/dd/span[2])",
    "growth":      "string(//*[@id='metrics-root']//dl[6]/dd/span[2])",
    "profitab":    "string(//*[@id='metrics-root']//dl[7]/dd/span[2])",
    "safety":      "string(//*[@id='metrics-root']//dl[8]/dd/span[2])",
    "risk":        "string(//*[@id='metrics-root']//dl[9]/dd/span[2])",
    "return_rate": "string(//*[@id='metrics-root']//dl[10]/dd/span[2])",
    "liquidity":   "string(//*[@id='metrics-root']//dl[11]/dd/span[2])",
    "trend":       "string(//*[@id='metrics-root']//dl[12]/dd/span[2])",
    "forex":       "string(//*[@id='metrics-root']//dl[13]/dd/span[2])",
    "technical":   "string(//*[@id='metrics-root']//dl[14]/dd/span[2])",
}

def _pctish(s: str) -> str:
    s = (s or "").strip()
    if s and not s.endswith("%") and any(k in s for k in ["成長", "率", "yield"]):
        # サンプル：パーセント想定の値に % を補う処理（任意）
        return s + "%"
    return s

class TokenBucket:
    """単一プロセス内の簡易トークンバケットで全体QPSを制御"""
    def __init__(self, qps: float):
        self.interval = 1.0 / max(qps, 0.0001)
        self._lock = asyncio.Lock()
        self._next_time = time.monotonic()
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next_time:
                await asyncio.sleep(self._next_time - now)
            self._next_time = max(now, self._next_time) + self.interval

@asynccontextmanager
async def browser_context(play, headless=True):
    browser = await play.chromium.launch(
        headless=headless,
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome Safari"
        ),
        java_script_enabled=True,
        bypass_csp=True,
        viewport={"width": 1366, "height": 768}
    )
    # 軽量化: 画像/フォント/メディア遮断（CSSとXHRは許可）
    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", route_handler)
    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one(page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    await page.wait_for_selector(SEL_READY, timeout=NAV_TIMEOUT_MS)
    # XPaths 一括評価（STRING_TYPE）
    data = await page.evaluate(
        """(xps) => {
            const out = {};
            const get = (xp) => {
              try {
                return document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null)
                               .stringValue.trim();
              } catch(e) { return ""; }
            };
            for (const [k, xp] of Object.entries(xps)) out[k] = get(xp);
            return out;
        }""",
        XPATHS
    )
    # 例: % 付与の簡易整形
    data["sales"]  = _pctish(data.get("sales", ""))
    data["profit"] = _pctish(data.get("profit", ""))
    return code, data

async def worker(worker_id: int, ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None:
                break
            code, url = item
            await bucket.acquire()
            delay = 0.8
            last_err = None
            for attempt in range(RETRIES):
                try:
                    c, data = await fetch_one(page, code, url)
                    await results.put((c, data, None))
                    break
                except (PwTimeout, Exception) as e:
                    last_err = e
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(delay)
                        delay *= 1.8
                    else:
                        await results.put((code, None, last_err))
            jobs.task_done()
    finally:
        await page.close()

def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_metrics (
            target_date TEXT,
            code        TEXT,
            rating      TEXT,
            sales       TEXT,
            profit      TEXT,
            scale       TEXT,
            cheap       TEXT,
            growth      TEXT,
            profitab    TEXT,
            safety      TEXT,
            risk        TEXT,
            return_rate TEXT,
            liquidity   TEXT,
            trend       TEXT,
            forex       TEXT,
            technical   TEXT,
            PRIMARY KEY (target_date, code)
        )
    """)
    conn.commit()

def resolve_target_date(conn: sqlite3.Connection, explicit: Optional[str]) -> Optional[str]:
    """明示が無ければ consensus_url の最新日付を採用（サンプル前提）"""
    if explicit:
        datetime.datetime.strptime(explicit, "%Y%m%d")
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(target_date) FROM consensus_url")
    row = cur.fetchone()
    td = row[0] if row else None
    if td:
        datetime.datetime.strptime(td, "%Y%m%d")
        return td
    return None

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[Tuple[str, str]]:
    """
    consensus_url(target_date, code, name, link_a, link_b, link_c) を前提に、
    指標ページURLは link_b を採用するサンプル。
    """
    cur = conn.cursor()
    if mode == "all":
        cur.execute("""
            SELECT code, link_b FROM consensus_url
            WHERE target_date = ?
        """, (target_date,))
        return [(c, u) for c, u in cur.fetchall() if u]

    # missing: market_metrics に未保存の code を抽出して link_b を取得
    cur.execute("""
        SELECT code FROM consensus_url WHERE target_date = ?
        EXCEPT
        SELECT code FROM market_metrics WHERE target_date = ?
    """, (target_date, target_date))
    codes = [r[0] for r in cur.fetchall()]
    if not codes:
        return []
    placeholders = ",".join(["?"] * len(codes))
    cur.execute(f"""
        SELECT code, link_b FROM consensus_url
        WHERE target_date = ? AND code IN ({placeholders})
    """, [target_date] + codes)
    return [(c, u) for c, u in cur.fetchall() if u]

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定なら consensus_url の最新日付）")
    p.add_argument("--mode", choices=["missing", "all"], default="missing",
                   help="missing: 未取得のみ / all: 全件")
    p.add_argument("--concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    p.add_argument("--qps", type=float, default=DEFAULT_TARGET_QPS)
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH_COMMIT)
    p.add_argument("--headful", action="store_true", help="ブラウザを表示（デバッグ用）")
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ target_date を決定できません（-a YYYYMMDD を指定するか、consensus_url にデータが必要）")
        conn.close()
        return
    print(f"▶ target_date = {target_date} / mode = {args.mode}")

    targets = load_targets(conn, target_date, args.mode)
    if not targets:
        msg = "未取得なし" if args.mode == "missing" else "対象URLなし"
        print(f"ℹ️ {target_date} {msg}")
        conn.close()
        return

    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    for item in targets:
        await jobs.put(item)
    total = len(targets)

    bucket = TokenBucket(args.qps)
    done = ok = ng = 0
    buf: List[Tuple] = []

    async with async_playwright() as play:
        async with browser_context(play, headless=not args.headful) as ctx:
            workers = [
                asyncio.create_task(worker(i + 1, ctx, jobs, bucket, results))
                for i in range(max(1, min(args.concurrency, 12)))
            ]

            async def stop_workers():
                for _ in workers:
                    await jobs.put(None)

            try:
                while done < total:
                    code, data, err = await results.get()
                    done += 1
                    if err is None and data:
                        row = (
                            target_date, code,
                            data.get("rating",""), data.get("sales",""), data.get("profit",""),
                            data.get("scale",""), data.get("cheap",""), data.get("growth",""),
                            data.get("profitab",""), data.get("safety",""), data.get("risk",""),
                            data.get("return_rate",""), data.get("liquidity",""),
                            data.get("trend",""), data.get("forex",""), data.get("technical","")
                        )
                        buf.append(row); ok += 1
                        if len(buf) >= args.batch:
                            cur.executemany("""
                                INSERT OR REPLACE INTO market_metrics (
                                    target_date, code, rating, sales, profit, scale, cheap, growth,
                                    profitab, safety, risk, return_rate, liquidity, trend, forex, technical
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, buf)
                            conn.commit()
                            buf.clear()
                    else:
                        ng += 1

                    if done % 50 == 0 or done == total:
                        print(f"📦 {done}/{total}  OK:{ok}  NG:{ng}")

            finally:
                if buf:
                    cur.executemany("""
                        INSERT OR REPLACE INTO market_metrics (
                            target_date, code, rating, sales, profit, scale, cheap, growth,
                            profitab, safety, risk, return_rate, liquidity, trend, forex, technical
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, buf)
                    conn.commit()
                await stop_workers()
                await asyncio.gather(*workers, return_exceptions=True)
                conn.close()
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total} / mode={args.mode} / date={target_date}")

if __name__ == "__main__":
    asyncio.run(main())
