from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Callable, Optional
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

"""Shared proxy layer: Webshare (authenticated) only — no public proxy lists."""

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_ENV_LOADED = False

_OK_STATUSES = frozenset({200, 201, 202, 203, 206})

_BLOCK_PATTERNS = re.compile(
    r"checking your browser|verify you are human|attention required|"
    r"access denied|request blocked|too many requests|"
    r"cf-browser-verification|cdn-cgi/challenge|"
    r"just a moment|enable javascript|"
    r"robot check|captcha",
    re.I,
)


class ProxyExhaustedError(RuntimeError):
    """All rotation attempts failed; no direct (no-proxy) fallback."""

    def __init__(self, url: str, message: str = "") -> None:
        self.url = url
        super().__init__(message or f"All proxies exhausted for {url}")


def _load_local_dotenv_once() -> None:
    """Load `.env` from this directory for standalone scraper runs.

    We only set missing keys so explicit shell env vars always win.
    """
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True

    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


_load_local_dotenv_once()

_FALSEISH = frozenset({"0", "false", "off", "no"})


def script_proxy_enabled(scraper_id: str) -> bool:
    """Return whether this scraper should use the Webshare proxy pool.

    Reads ``{SCRAPER_ID_UPPER}_USE_PROXY`` (e.g. ``EC21_USE_PROXY``,
    ``MADE_IN_CHINA_USE_PROXY``). When that variable is **unset**, falls back to
    ``SCRAPER_PROXY_ENABLED`` (default ``1`` = enabled).

    ``scraper_id`` must match the name passed to :func:`create_proxy_pool`
    (e.g. ``\"ec21\"``, ``\"made_in_china\"``, ``\"kompass\"``).
    """
    key = f"{scraper_id.upper()}_USE_PROXY"
    raw = os.getenv(key)
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip().lower() not in _FALSEISH
    return os.getenv("SCRAPER_PROXY_ENABLED", "1").strip().lower() not in _FALSEISH


@dataclass(frozen=True)
class ProxyEndpoint:
    host_port: str
    anonymity: str = "HIA"
    source: str = "webshare"
    username: Optional[str] = None
    password: Optional[str] = None

    @property
    def server(self) -> str:
        return f"http://{self.host_port}"

    def urllib_proxy_url(self) -> str:
        """Proxy URL for urllib ``ProxyHandler`` (includes credentials when present)."""
        if self.username:
            u = quote(self.username, safe="")
            p = quote(self.password or "", safe="")
            return f"http://{u}:{p}@{self.host_port}/"
        return self.server


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except ValueError:
        return default


def _extract_status_and_body(response: Any) -> tuple[int, str]:
    status = getattr(response, "status", None) or getattr(response, "status_code", 0) or 0
    try:
        status = int(status)
    except (TypeError, ValueError):
        status = 0
    body = getattr(response, "body", None)
    if isinstance(body, (bytes, bytearray)):
        text = body.decode("utf-8", errors="ignore")
    elif isinstance(body, str):
        text = body
    else:
        text = getattr(response, "text", None) or ""
    if not text and hasattr(response, "text"):
        text = response.text or ""
    return status, text or ""


def _is_response_valid(
    status: int,
    body: str,
    validator: Optional[Callable[[str], bool]],
    min_body_bytes: int,
) -> bool:
    if status not in _OK_STATUSES:
        return False
    if len(body) < min_body_bytes:
        return False
    if _BLOCK_PATTERNS.search(body):
        return False
    if validator is not None and not validator(body):
        return False
    return True


def resolve_webshare_config() -> Optional[tuple[str, str, str, str]]:
    """Return ``(host, port, user, password)`` from environment.

    Reads ``SCRAPER_WEBSHARE_*`` first, then ``KOMPASS_WEBSHARE_*`` for compatibility.
    Host defaults to ``p.webshare.io``, port to ``80``.
    """
    host = (
        os.getenv("SCRAPER_WEBSHARE_HOST", "").strip()
        or os.getenv("KOMPASS_WEBSHARE_HOST", "").strip()
        or "p.webshare.io"
    )
    port = (
        os.getenv("SCRAPER_WEBSHARE_PORT", "").strip()
        or os.getenv("KOMPASS_WEBSHARE_PORT", "").strip()
        or "80"
    )
    user = (
        os.getenv("SCRAPER_WEBSHARE_USER", "").strip()
        or os.getenv("KOMPASS_WEBSHARE_USER", "").strip()
    )
    pw = (
        os.getenv("SCRAPER_WEBSHARE_PASS", "").strip()
        or os.getenv("KOMPASS_WEBSHARE_PASS", "").strip()
        or os.getenv("KOMPASS_WEBSHARE_PASSWORD", "").strip()
    )
    if not user or not pw:
        return None
    return (host, port, user, pw)


class ProxyPool:
    """Single Webshare endpoint per pool (rotation is handled by Webshare on their side)."""

    def __init__(
        self,
        name: str,
        webshare: tuple[str, str, str, str],
        *,
        refresh_seconds: int = 900,
        timeout_seconds: int = 20,
    ) -> None:
        self.name = name
        self._webshare = webshare
        self.refresh_seconds = max(120, int(refresh_seconds))
        self.timeout_seconds = max(5, int(timeout_seconds))
        self._lock = RLock()
        self._last_refresh = 0.0
        self._proxies: list[ProxyEndpoint] = []
        self._cooldown_until: dict[str, float] = {}
        self._success: dict[str, int] = {}
        self._fail: dict[str, int] = {}
        self._idx = 0
        self._mid_refresh_done = False

    def refresh(self, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            if (
                not force
                and self._proxies
                and now - self._last_refresh < self.refresh_seconds
            ):
                return

        host, port, user, pw = self._webshare
        host = host.strip()
        port = str(port).strip()
        user = user.strip()
        pw = str(pw)
        host_port = f"{host}:{port}"
        parsed = [
            ProxyEndpoint(
                host_port=host_port,
                anonymity="HIA",
                source="webshare",
                username=user,
                password=pw,
            )
        ]
        with self._lock:
            self._proxies = parsed
            self._last_refresh = now
            self._cooldown_until = {}
            self._success = {}
            self._fail = {}
            self._idx = 0
            self._mid_refresh_done = False
        safe_user = user[:3] + "***" if len(user) > 3 else "***"
        print(
            f"[PROXY][{self.name}] Webshare pool ready "
            f"(host={host_port}, user={safe_user})"
        )

    def _eligible(self, now: float) -> list[ProxyEndpoint]:
        proxies = list(self._proxies)
        return [p for p in proxies if self._cooldown_until.get(p.host_port, 0.0) <= now]

    def _score(self, p: ProxyEndpoint) -> int:
        return self._success.get(p.host_port, 0) - self._fail.get(p.host_port, 0)

    def get(self) -> Optional[ProxyEndpoint]:
        self.refresh()
        now = time.time()
        with self._lock:
            if not self._proxies:
                return None
            eligible = self._eligible(now) or self._proxies
            if not eligible:
                return None
            healthy = [p for p in eligible if self._score(p) >= 0]
            choices = healthy or eligible
            choices = sorted(choices, key=self._score, reverse=True)
            self._idx = (self._idx + 1) % len(choices)
            return choices[self._idx]

    def mark_bad(self, endpoint: Optional[ProxyEndpoint], cooldown_seconds: int = 180) -> None:
        if endpoint is None:
            return
        with self._lock:
            self._cooldown_until[endpoint.host_port] = time.time() + max(30, cooldown_seconds)
            self._fail[endpoint.host_port] = self._fail.get(endpoint.host_port, 0) + 1

    def mark_good(self, endpoint: Optional[ProxyEndpoint]) -> None:
        if endpoint is None:
            return
        with self._lock:
            self._success[endpoint.host_port] = self._success.get(endpoint.host_port, 0) + 1

    def _bad_fraction_unlocked(self) -> float:
        if not self._proxies:
            return 0.0
        now = time.time()
        bad = sum(1 for p in self._proxies if self._cooldown_until.get(p.host_port, 0) > now)
        return bad / max(1, len(self._proxies))

    def maybe_mid_refresh(self) -> None:
        with self._lock:
            if self._mid_refresh_done or not self._proxies:
                return
            if self._bad_fraction_unlocked() < 0.5:
                return
            self._mid_refresh_done = True
        print(f"[PROXY][{self.name}] >=50% proxies in cooldown; resetting pool state")
        self.refresh(force=True)

    def playwright_config(self, endpoint: Optional[ProxyEndpoint] = None) -> Optional[dict[str, str]]:
        ep = endpoint or self.get()
        if ep is None:
            return None
        cfg: dict[str, str] = {"server": ep.server}
        if ep.username is not None:
            cfg["username"] = ep.username
            cfg["password"] = ep.password or ""
        return cfg

    def _probe_one(self, endpoint: ProxyEndpoint) -> bool:
        proxy_url = endpoint.urllib_proxy_url()
        opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
        req = Request(
            "https://httpbin.org/ip",
            headers={"User-Agent": UA},
        )
        try:
            with opener.open(req, timeout=6) as resp:
                body = resp.read()
            return len(body) > 20
        except Exception:
            return False

    def maybe_warmup(self) -> None:
        if os.getenv("SCRAPER_PROXY_WARMUP", "").strip().lower() not in {"1", "true", "yes", "on"}:
            return
        with self._lock:
            candidates = list(self._proxies)
        if not candidates:
            return
        print(f"[PROXY][{self.name}] warmup: probing Webshare endpoint...")
        alive: list[ProxyEndpoint] = []
        with ThreadPoolExecutor(max_workers=min(4, len(candidates))) as ex:
            fut_map = {ex.submit(self._probe_one, p): p for p in candidates}
            for fut in as_completed(fut_map):
                p = fut_map[fut]
                try:
                    if fut.result():
                        alive.append(p)
                except Exception:
                    pass
        if not alive:
            print(f"[PROXY][{self.name}] warmup: probe failed; keeping pool (Webshare may still work)")
            return
        with self._lock:
            self._proxies = alive
            self._cooldown_until = {}
            self._success = {}
            self._fail = {}
            self._idx = 0
        print(f"[PROXY][{self.name}] warmup: OK")


def create_proxy_pool(scraper_name: str) -> Optional[ProxyPool]:
    if not script_proxy_enabled(scraper_name):
        return None

    ws = resolve_webshare_config()
    if not ws:
        print(
            "[PROXY][WARN] SCRAPER_PROXY_ENABLED but Webshare credentials missing "
            "(set SCRAPER_WEBSHARE_USER and SCRAPER_WEBSHARE_PASS, or KOMPASS_WEBSHARE_*). "
            "Pool disabled for this process."
        )
        return None

    refresh_seconds = int(os.getenv("SCRAPER_PROXY_REFRESH_SECONDS", "900") or "900")
    timeout_seconds = int(os.getenv("SCRAPER_PROXY_TIMEOUT_SECONDS", "20") or "20")
    pool = ProxyPool(
        name=scraper_name,
        webshare=ws,
        refresh_seconds=refresh_seconds,
        timeout_seconds=timeout_seconds,
    )
    pool.refresh(force=True)
    pool.maybe_warmup()
    return pool


def _urllib_fetch_via_proxy(url: str, headers: Optional[dict[str, str]], endpoint: ProxyEndpoint) -> Any:
    proxy_url = endpoint.urllib_proxy_url()
    opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
    req_headers = {"User-Agent": UA}
    if headers:
        req_headers.update(headers)
    req = Request(url, headers=req_headers)
    with opener.open(req, timeout=25) as resp:
        body = resp.read()
    status = getattr(resp, "status", 200)
    try:
        status = int(status)
    except (TypeError, ValueError):
        status = 200
    return type(
        "ProxyResponse",
        (),
        {
            "status": status,
            "status_code": status,
            "body": body,
            "text": body.decode("utf-8", errors="ignore"),
            "url": getattr(resp, "url", url),
        },
    )()


def fetch_with_proxy_rotation(
    fetcher: Any,
    url: str,
    headers: Optional[dict[str, str]],
    pool: Optional[ProxyPool],
    attempts: Optional[int] = None,
    validator: Optional[Callable[[str], bool]] = None,
) -> Any:
    min_body = _env_int("SCRAPER_PROXY_MIN_BODY_BYTES", 500)
    if attempts is None:
        attempts = _env_int("SCRAPER_PROXY_ATTEMPTS", 6)
    attempts = max(1, attempts)

    last_exc: Optional[Exception] = None

    if pool is None:
        if headers:
            return fetcher.fetch(url, headers=headers)
        return fetcher.fetch(url)

    for _ in range(attempts):
        endpoint = pool.get()
        if endpoint is None:
            pool.maybe_mid_refresh()
            raise ProxyExhaustedError(url, "Proxy pool is empty")

        try:
            response = _urllib_fetch_via_proxy(url, headers, endpoint)
            status, body = _extract_status_and_body(response)
            if _is_response_valid(status, body, validator, min_body):
                pool.mark_good(endpoint)
                return response
            pool.mark_bad(endpoint, cooldown_seconds=120)
        except Exception as exc:
            last_exc = exc
            pool.mark_bad(endpoint)

    pool.maybe_mid_refresh()
    if last_exc is not None:
        raise ProxyExhaustedError(url, f"All proxies exhausted for {url}: {last_exc!r}") from last_exc
    raise ProxyExhaustedError(url)


def goto_with_rotation(
    page: Any,
    url: str,
    pool: Optional[ProxyPool],
    relaunch_fn: Callable[[Optional[ProxyEndpoint]], Any],
    *,
    current_endpoint: Optional[ProxyEndpoint] = None,
    timeout_ms: int = 45000,
    wait_until: str = "domcontentloaded",
    validate: Optional[Callable[[str], bool]] = None,
    max_relaunches: Optional[int] = None,
) -> tuple[Any, Optional[ProxyEndpoint]]:
    """Hybrid: retry page.goto once on same browser, then relaunch with new proxy.

    Returns ``(page, endpoint)`` where ``endpoint`` is the proxy used for the
    returned page (for the caller to pass back as ``current_endpoint`` on later
    calls). Pass ``current_endpoint`` from the previous return so a failed cycle
    can mark that proxy bad before relaunching.
    """
    if pool is None:
        page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        return page, None

    if max_relaunches is None:
        max_relaunches = _env_int("SCRAPER_PROXY_BROWSER_RELAUNCHES", 4)
    max_relaunches = max(1, max_relaunches)
    min_body = _env_int("SCRAPER_PROXY_MIN_BODY_BYTES", 200)

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    except ImportError:
        PlaywrightTimeoutError = TimeoutError  # type: ignore[misc,assignment]

    def _html_ok(html: str) -> bool:
        if len(html) < min_body:
            return False
        if _BLOCK_PATTERNS.search(html):
            return False
        if validate and not validate(html):
            return False
        return True

    current = page
    ep_in_use = current_endpoint
    for relaunch_idx in range(max_relaunches):
        for inner in range(2):
            try:
                resp = current.goto(url, wait_until=wait_until, timeout=timeout_ms)
                status = resp.status if resp is not None else 200
                html = current.content()
                if status in _OK_STATUSES and _html_ok(html):
                    if ep_in_use:
                        pool.mark_good(ep_in_use)
                    return current, ep_in_use
            except PlaywrightTimeoutError:
                pass
            except Exception:
                pass
            if inner == 0:
                continue
            break

        if relaunch_idx < max_relaunches - 1:
            if ep_in_use is not None:
                pool.mark_bad(ep_in_use, cooldown_seconds=240)
            ep = pool.get()
            if ep is None:
                pool.maybe_mid_refresh()
                raise ProxyExhaustedError(url)
            current = relaunch_fn(ep)
            ep_in_use = ep

    pool.maybe_mid_refresh()
    raise ProxyExhaustedError(url)
