import html
import json
import re
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType
from app.utils.http import RequestUtils


class ZmptKeeperCheck(_PluginBase):
    """ZMPT 保种组检查：定时抓取组员官种体积，判定合格/不合格，结果推送通知。"""

    # ===== 插件元信息（必须与 package.v2.json 完全一致）=====
    plugin_name = "ZMPT保种组检查"
    plugin_desc = "定时抓取ZMPT保种组官种体积，判定合格/不合格；结果推送到通知渠道。"
    plugin_icon = "Moviepilot_A.png"
    plugin_version = "1.1.0"
    plugin_author = "2536003090"
    author_url = "https://github.com/2536003090"
    plugin_config_prefix = "zmptkeeper_"
    plugin_order = 28
    auth_level = 1

    # ===== 固定组配置（不在UI显示）=====
    _DEFAULT_GROUPS = [
        {"id": "6", "name": "5T组", "threshold": 5.0},
        {"id": "10", "name": "10T组", "threshold": 10.0},
    ]

    # ===== 运行时状态 =====
    _enabled = False
    _onlyonce = False
    _cookie = ""
    _base = "https://zmpt.cc"
    _cron = "0 8 * * *"
    _notify = True
    _delay = 1.0
    _groups = []
    _last_result = ""
    _member_url = ""  # 组员列表URL模板，支持 {id} 和 {page} 占位符；空则用内置默认
    _use_browser = False  # 用 MP 内置浏览器(Playwright)渲染页面，抓 JS 动态加载的组员
    _scheduler = None  # “立即运行一次”用的一次性调度器

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._cookie = (config.get("cookie") or "").strip()
        self._cron = (config.get("cron") or "0 8 * * *").strip()
        self._notify = config.get("notify") is not False
        try:
            self._delay = float(config.get("delay") or 1.0)
        except Exception:
            self._delay = 1.0
        self._groups = [dict(g) for g in self._DEFAULT_GROUPS]
        self._member_url = (config.get("member_url") or "").strip()
        self._use_browser = bool(config.get("use_browser"))
        logger.info(f"ZMPT保种组检查 已加载：enabled={self._enabled} cron={self._cron} onlyonce={self._onlyonce} use_browser={self._use_browser} member_url={self._member_url or '(默认)'}")

        # 立即运行一次：开关打开并保存后，3秒后触发一次检查，随后自动关闭开关
        if self._enabled and self._onlyonce:
            self._onlyonce = False
            try:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=self.check,
                    trigger="date",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    id="ZmptKeeperCheck.RunOnce",
                    name="ZMPT保种组检查立即运行",
                )
                self.update_config(self._build_config())
                self._scheduler.print_jobs()
                self._scheduler.start()
                logger.info("ZMPT保种组检查：已计划立即运行一次")
            except Exception as e:
                logger.error(f"ZMPT保种组检查：立即运行调度失败: {e}")
                self.update_config(self._build_config())

    def _build_config(self) -> dict:
        """构造当前配置字典，用于 update_config 回写（如把 onlyonce 开关关掉）。"""
        return {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cookie": self._cookie,
            "cron": self._cron,
            "notify": self._notify,
            "delay": self._delay,
            "member_url": self._member_url,
            "use_browser": self._use_browser,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        # 注册 /zmpt_check 命令，方便手动触发，不用等 cron
        return [{
            "cmd": "/zmpt_check",
            "event": EventType.PluginAction,
            "desc": "立即执行ZMPT保种组检查",
            "category": "插件命令",
            "data": {"action": "zmpt_keeper_check_run"},
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """定时任务：用 CronTrigger.from_crontab 解析 cron5 表达式"""
        if not self.get_state() or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.error(f"ZMPT cron 表达式非法: {self._cron} -> {e}")
            return []
        return [{
            "id": "ZmptKeeperCheck.Check",
            "name": "ZMPT保种组检查",
            "trigger": trigger,
            "func": self.check,
            "kwargs": {},
        }]

    @eventmanager.register(EventType.PluginAction)
    def _on_action(self, event: Event):
        if (event.event_data or {}).get("action") != "zmpt_keeper_check_run":
            return
        logger.info("ZMPT保种组检查：收到手动触发命令 /zmpt_check")
        self.check()

    def stop_service(self):
        # 关闭“立即运行一次”的一次性调度器（cron 定时任务由框架统一管理）
        try:
            if self._scheduler and self._scheduler.running:
                self._scheduler.shutdown(wait=False)
        except Exception as e:
            logger.warn(f"ZMPT保种组检查：停止调度器失败: {e}")
        self._scheduler = None

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页：返回 (页面JSON, 默认配置)。model 必须放在 props 内。"""
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "notify", "label": "推送通知"}},
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {"model": "cookie", "label": "ZMPT Cookie", "rows": 3,
                            "placeholder": "浏览器登录 zmpt.cc 后，F12 复制整串 Cookie（至少含 session）"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VCronField", "props": {"model": "cron",
                            "label": "定时执行周期（cron）", "placeholder": "0 8 * * *（每天8点）"}},
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {"model": "delay", "label": "请求间隔(秒)", "placeholder": "1.0"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VSwitch", "props": {"model": "use_browser",
                            "label": "用内置浏览器渲染页面（组员是JS动态加载/懒加载时打开，较慢）"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextField", "props": {"model": "member_url",
                            "label": "组员列表URL模板（可选，含 {id} 和 {page}）",
                            "placeholder": "留空用默认；自定义形如 https://zmpt.cc/xxx.php?id={id}&page={page}"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                            "text": "打开\"立即运行一次\"开关并保存，3秒后执行一次检查（执行完会自动关闭）。组配置已内置（5T组id=6 / 10T组id=10），无需填写。"}},
                    ]},
                ]},
            ]},
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": True,
            "cookie": "",
            "cron": "0 8 * * *",
            "delay": 1.0,
            "member_url": "",
            "use_browser": False,
        }

    def get_page(self) -> List[dict]:
        if not self._last_result:
            return [
                {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                    "text": "尚未运行。配置好 Cookie 后，发送 /zmpt_check 立即执行，或等待定时触发。"}},
            ]
        return [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": self._last_result}},
                ]},
            ]},
        ]

    # ====================== 核心流程 ======================
    def check(self):
        if not self._enabled:
            return
        if not self._cookie:
            logger.warn("ZMPT保种组检查：未配置 Cookie")
            self._last_result = "⚠️ 未配置 Cookie，无法抓取。"
            self._notify_msg("ZMPT保种组检查", "⚠️ 未配置 Cookie，无法抓取。请在插件设置填入 zmpt.cc 的 Cookie。")
            return
        logger.info("ZMPT保种组检查：开始执行")
        summary = []
        for g in self._groups:
            try:
                summary.append(self._check_group(g))
            except Exception as e:
                logger.error(f"ZMPT保种组检查 组{g.get('id')} 出错: {e}")
                logger.error(traceback.format_exc())
                summary.append(f"{g.get('name', '组' + str(g.get('id')))}：执行出错 {e}")
        self._last_result = "\n\n".join(summary) if summary else "无组配置"
        logger.info("ZMPT保种组检查：执行完成")

    def _check_group(self, g):
        users, diag = self._fetch_users(g["id"])
        if not users:
            msg = f"[插件v{self.plugin_version}{'/浏览器' if self._use_browser else '/普通'}] ⚠️ {g['name']}：未抓到组员（组id={g['id']}）。\n诊断：{diag}"
            self._notify_msg(f"ZMPT {g['name']}", msg)
            return msg
        rows = []
        ok_n = bad_n = err_n = 0
        for u in users:
            vol = self._fetch_volume(u["id"])
            if vol is None:
                err_n += 1
                status, volstr, intt = "异常", "未知", "-"
            else:
                intt = int(vol)
                volstr = f"{vol:.3f} TB"
                if vol >= g["threshold"]:
                    ok_n += 1
                    status = "合格"
                else:
                    bad_n += 1
                    status = "不合格"
            rows.append({"id": u["id"], "name": u["name"], "level": u["level"],
                         "vol": volstr, "intt": intt, "status": status})
            time.sleep(self._delay)
        text = self._format_text(g, rows, ok_n, bad_n, err_n)
        self._notify_msg(f"ZMPT {g['name']} 审查结果", text)
        summary = f"[v{self.plugin_version}{'/浏览器' if self._use_browser else '/普通'}] 【{g['name']}】共 {len(rows)} 人 · 合格 {ok_n} / 不合格 {bad_n} / 异常 {err_n}"
        return summary

    def _format_text(self, g, rows, ok_n, bad_n, err_n):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [
            f"插件版本：v{self.plugin_version}（{'浏览器模式' if self._use_browser else '普通模式'}）",
            f"抓取时间：{now}",
            f"{g['name']} · 阈值 ≥ {g['threshold']:.0f} TB 合格 · 共 {len(rows)} 人（合格 {ok_n} / 不合格 {bad_n} / 异常 {err_n}）",
        ]
        lines.append("")
        lines.append("ID\t用户名\t等级\t官种体积\t取整T\t结果")
        for r in rows:
            lines.append(f"{r['id']}\t{r['name']}\t{r['level'] or '-'}\t{r['vol']}\t{r['intt']}\t{r['status']}")
        return "\n".join(lines)

    # ====================== HTTP ======================
    def _cookie_dict(self):
        d = {}
        if not self._cookie:
            return d
        for pair in self._cookie.split(";"):
            if "=" in pair:
                k, v = pair.strip().split("=", 1)
                if k.strip():
                    d[k.strip()] = v.strip()
        return d

    @staticmethod
    def _headers():
        return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    def _http_get_diag(self, url):
        """返回 (text, status, length)；非 200 时 text=None。供诊断用。"""
        try:
            res = RequestUtils(cookies=self._cookie_dict(), timeout=30,
                               headers=self._headers()).get_res(url)
            status = getattr(res, "status_code", None)
            length = len(res.text) if res is not None else 0
            text = res.text if (res is not None and status == 200) else None
            if text is None and status is not None:
                logger.warn(f"ZMPT请求 非200 {url}: status={status}")
            return text, status, length
        except Exception as e:
            logger.warn(f"ZMPT请求失败 {url}: {e}")
            return None, None, 0

    def _http_get(self, url):
        return self._http_get_diag(url)[0]

    @staticmethod
    def _sample_user_links(html):
        """提取若干疑似用户主页链接样本，供诊断输出，定位组员链接格式。"""
        soup = BeautifulSoup(html, "html.parser")
        samples, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith(("#", "javascript")):
                continue
            if ZmptKeeperCheck._extract_uid(href):
                if href not in seen:
                    seen.add(href)
                    samples.append(href)
            if len(samples) >= 8:
                break
        return samples

    def _build_diag(self, url, text, status, length):
        """构造可读的诊断串，帮助定位 抓不到组员 的原因。"""
        if status is None:
            return f"请求失败/超时（网络异常或请求被拦截）| URL={url}"
        parts = [f"HTTP {status}", f"长度 {length}"]
        if text:
            low = text.lower()
            # 登录页判定：必须有登录表单特征，避免导航栏“登录”字样误判
            is_login = ("takelogin" in low
                        or ('name="username"' in low and "password" in low)
                        or ("login" in low and "password" in low and "<form" in low))
            parts.append("疑似登录页（Cookie 可能已失效）" if is_login else "页面正常（非登录页）")
            samples = self._sample_user_links(text)
            if samples:
                parts.append("疑似用户链接样本: " + " | ".join(samples))
            else:
                parts.append("静态HTML无用户链接")
            parts.append("Livewire: " + self._snapshot_summary(text))
        elif status == 200:
            parts.append("HTTP 200 但响应为空")
        return " | ".join(parts) + f" | URL={url}"

    # ====================== 抓组员 ======================
    def _fetch_users(self, role_id):
        # 浏览器模式：渲染页面(JS执行→Livewire表格加载→组员出现)，再解析
        if self._use_browser:
            return self._fetch_users_browser(role_id)
        users, seen = [], set()
        diag = ""
        page = 1
        while page <= 50:
            if self._member_url:
                url = self._member_url.replace("{id}", str(role_id)).replace("{page}", str(page))
            else:
                url = f"{self._base}/nexusphp/roles/{role_id}/edit?page={page}"
            html, status, length = self._http_get_diag(url)
            if page == 1:
                diag = self._build_diag(url, html, status, length)
            if not html:
                break
            page_users = self._parse_users(html, role_id=role_id)
            if not page_users:
                break
            new = 0
            for u in page_users:
                if u["id"] not in seen:
                    seen.add(u["id"])
                    users.append(u)
                    new += 1
            if new == 0:
                break
            page += 1
            time.sleep(self._delay)
        return users, diag

    def _fetch_users_browser(self, role_id):
        """用 MP 内置浏览器渲染组员页(执行Livewire/Filament JS)，返回 (users, diag)。"""
        url = (self._member_url.replace("{id}", str(role_id)).replace("{page}", "1")
               if self._member_url else f"{self._base}/nexusphp/roles/{role_id}/edit?page=1")
        html = self._render_with_browser(url)
        if not html:
            return [], f"内置浏览器渲染失败/超时（请确认MP已安装Playwright浏览器内核）| URL={url}"
        users = self._parse_users(html, role_id=role_id)
        diag = "[浏览器模式] " + self._build_diag(url, html, 200, len(html))
        return users, diag

    def _render_with_browser(self, url):
        """调用 PlaywrightHelper 渲染页面：加载后反复滚到底部触发懒加载，再返回完整HTML。"""
        try:
            from app.helper.browser import PlaywrightHelper
        except Exception as e:
            logger.warn(f"ZMPT PlaywrightHelper 不可用: {e}")
            return None

        def _scroll_and_read(page):
            try:
                page.set_default_timeout(20000)
            except Exception:
                pass
            # 关键修复：把 Cookie 写入浏览器 cookie jar（不只是 HTTP header），
            # 这样页面里的 JS / Livewire / Filament 发 AJAX 时才能正确带 session 与 CSRF，表格才会加载。
            try:
                jar = [{"name": k, "value": v, "domain": "zmpt.cc", "path": "/"}
                       for k, v in self._cookie_dict().items()]
                if jar:
                    page.context.add_cookies(jar)
            except Exception as e:
                logger.warn(f"ZMPT 写入浏览器cookie失败: {e}")
            # 重新加载，使 cookie jar 生效
            try:
                page.goto(url)
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            # 尝试直接调用 Livewire/Filament 触发表格加载
            try:
                page.evaluate("""() => {
                    try {
                        const L = window.Livewire;
                        if (L) {
                            const comps = L.all ? L.all() : (L.getComponents ? L.getComponents() : []);
                            comps.forEach(c => ['loadTable','loadRecords'].forEach(m => { try { c.call(m); } catch(e){} }));
                        }
                    } catch(e){}
                }""")
            except Exception:
                pass
            # 增量缓慢滚动，让表格进入视口触发 IntersectionObserver 懒加载
            try:
                page.evaluate("""() => new Promise(async (resolve) => {
                    const total = document.body.scrollHeight;
                    for (let y = 0; y <= total + 800; y += 350) {
                        window.scrollTo(0, y);
                        await new Promise(r => setTimeout(r, 350));
                    }
                    resolve();
                })""")
            except Exception:
                pass
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            try:
                return page.content()
            except Exception:
                return None

        try:
            return PlaywrightHelper().action(url, _scroll_and_read,
                                             cookies=self._cookie, headless=True, timeout=180)
        except Exception as e:
            logger.warn(f"ZMPT 浏览器渲染失败: {e}")
            return None

    @staticmethod
    def _extract_uid(href):
        """从用户主页链接里提取数字ID，兼容 nexusphp 与 Laravel 风格。"""
        if not href:
            return None
        # ?id=123 / &userid=123 / &uid=123
        m = re.search(r'[?&](?:id|userid|uid)=(\d+)', href, re.I)
        if m:
            return m.group(1)
        # /user/123 /users/123 /profile/123 /member/123 （路径段为纯数字）
        m = re.search(r'/(?:users?|profile|member)/(\d+)(?:[/?#]|$)', href, re.I)
        if m:
            return m.group(1)
        return None

    def _parse_users(self, html, role_id=None):
        """先按页面 <a> 链接解析；链接找不到时回退到 Livewire snapshot 取组员。"""
        users = self._parse_users_from_links(html)
        if users:
            return users
        return self._users_from_snapshots(html, exclude_id=str(role_id) if role_id else None)

    def _parse_users_from_links(self, html_text):
        soup = BeautifulSoup(html_text, "html.parser")
        users = []
        seen = set()
        for a in soup.find_all("a", href=True):
            uid = self._extract_uid(a["href"])
            if not uid or uid in seen:
                continue
            seen.add(uid)
            name = a.get_text(strip=True) or f"id:{uid}"
            tr = a.find_parent("tr")
            table = tr.find_parent("table") if tr else None
            level = self._level_from_tr(table, tr) if table else ""
            users.append({"id": uid, "name": name, "level": level, "href": a["href"]})
        return users

    # ---------- Livewire snapshot 解析（zmpt 用 Laravel/Livewire，组员在 wire:snapshot 的 JSON 里）----------
    _SNAPSHOT_ATTRS = ("wire:snapshot", "wire:initial-data")
    _UID_KEYS = ("id", "uid", "user_id", "userid")
    _NAME_KEYS = ("username", "name", "user_name", "uname", "nick", "nickname")

    def _iter_snapshots(self, html_text):
        """解码页面里所有 Livewire snapshot（wire:snapshot / wire:initial-data），产出 dict。"""
        soup = BeautifulSoup(html_text, "html.parser")
        for attr in self._SNAPSHOT_ATTRS:
            for el in soup.find_all(attrs={attr: True}):
                raw = el.get(attr)
                if not raw:
                    continue
                try:
                    yield json.loads(html.unescape(raw))
                except Exception:
                    continue

    def _users_from_snapshots(self, html_text, exclude_id=None):
        users = []
        seen = set()
        try:
            snaps = list(self._iter_snapshots(html_text))
            for uid, name in self._scan_user_collection(snaps):
                if exclude_id and uid == exclude_id:
                    continue  # 跳过“当前编辑的角色”自身，避免把角色对象当成组员
                if uid in seen:
                    continue
                seen.add(uid)
                users.append({"id": uid, "name": name or f"id:{uid}", "level": "", "href": ""})
        except Exception as e:
            logger.warn(f"ZMPT Livewire snapshot 解析失败: {e}")
        return users

    def _scan_user_collection(self, obj, out=None):
        """递归扫描 snapshot，找出形似"用户集合"的列表，收集 (uid, name)。"""
        if out is None:
            out = []
        if isinstance(obj, list):
            if obj and all(isinstance(x, dict) for x in obj[:6]):
                idkey = namekey = None
                for x in obj[:6]:
                    if idkey is None:
                        for k in self._UID_KEYS:
                            if k in x:
                                idkey = k
                                break
                    if namekey is None:
                        for k in self._NAME_KEYS:
                            if k in x:
                                namekey = k
                                break
                if idkey:
                    for x in obj:
                        if isinstance(x, dict):
                            v = x.get(idkey)
                            if v is not None and str(v).strip().isdigit():
                                nm = str(x.get(namekey, "")).strip() if namekey else ""
                                out.append((str(v).strip(), nm))
            for x in obj:
                self._scan_user_collection(x, out)
        elif isinstance(obj, dict):
            for v in obj.values():
                self._scan_user_collection(v, out)
        return out

    def _snapshot_summary(self, html_text):
        """诊断用：详细打印 Livewire snapshot 的 data 结构，重点暴露每个列表的字段名，用于定位组员。"""
        try:
            snaps = list(self._iter_snapshots(html_text))
        except Exception as e:
            return f"snapshot读取异常: {e}"
        if not snaps:
            return "无 wire:snapshot（页面可能不是Livewire，或snapshot在别处）"
        parts = []
        for si, snap in enumerate(snaps[:2]):
            if not isinstance(snap, dict):
                parts.append(f"#{si} 非dict")
                continue
            data = snap.get("data")
            if not isinstance(data, dict):
                sm = snap.get("serverMemo")
                data = sm.get("data") if isinstance(sm, dict) else None
            if not isinstance(data, dict):
                parts.append(f"#{si} topkeys={list(snap.keys())}（无data）")
                continue
            bits = [f"#{si} datakeys={list(data.keys())}"]
            for k, v in data.items():
                lst = v if isinstance(v, list) else (
                    v.get("data") if isinstance(v, dict) and isinstance(v.get("data"), list) else None)
                if isinstance(lst, list):
                    fields = list(lst[0].keys())[:12] if lst and isinstance(lst[0], dict) else "(非dict)"
                    bits.append(f"「{k}」=列表[{len(lst)}项]字段{fields}")
                elif isinstance(v, dict):
                    bits.append(f"「{k}」=dict{list(v.keys())[:8]}")
                else:
                    bits.append(f"「{k}」={str(v)[:30]}")
            parts.append(" ".join(bits))
        return " || ".join(parts)

    def _level_from_tr(self, table, tr):
        if not table or not tr:
            return ""
        head = table.find("tr")
        if not head:
            return ""
        lvl_col = -1
        for i, c in enumerate(head.find_all(["th", "td"])):
            txt = c.get_text(strip=True)
            if any(kw in txt for kw in ("等级", "用户组", "class", "level", "group")):
                lvl_col = i
                break
        if lvl_col < 0:
            return ""
        cells = tr.find_all("td")
        return cells[lvl_col].get_text(strip=True) if lvl_col < len(cells) else ""

    # ====================== 抓官种体积 ======================
    def _fetch_volume(self, uid):
        html = self._http_get(f"{self._base}/userdetails.php?id={uid}")
        return self._extract_volume(html) if html else None

    def _extract_volume(self, html):
        soup = BeautifulSoup(html, "html.parser")
        scope, scope_len = None, None
        for el in soup.select("tr, table, tbody, dl, div, section, fieldset"):
            try:
                txt = el.get_text()
            except Exception:
                continue
            if "官种加成" in txt and len(txt) < 600:
                s = len(str(el))
                if scope is None or s < scope_len:
                    scope, scope_len = el, s
        if scope:
            for c in scope.select("td, th, dd, li, span, div"):
                t = c.get_text(strip=True)
                if re.match(r'^\d[\d,]*\.?\d*\s*(PB|TB|GB|MB|KB|B)$', t, re.I):
                    v = self._parse_size_tb(t)
                    if v is not None:
                        return v
        text = soup.get_text(" ", strip=True)
        idx = text.find("官种加成")
        if idx < 0:
            idx = text.find("官种")
        if idx >= 0:
            seg = text[idx:idx + 200]
            m = re.search(r'(\d+\.?\d*)\s*(PB|TB|GB|MB|KB|B)', seg, re.I)
            if m:
                return self._parse_size_tb(m.group(1) + m.group(2))
        return None

    @staticmethod
    def _parse_size_tb(text):
        if text is None:
            return None
        s = re.sub(r',', '', str(text)).strip()
        m = re.match(r'([-+]?\d*\.?\d+)\s*(PB|TB|GB|MB|KB|TiB|GiB|MiB|PiB|B)?', s, re.I)
        if not m:
            return None
        try:
            num = float(m.group(1))
        except Exception:
            return None
        unit = (m.group(2) or "TB").upper().replace("IB", "B")
        factor = {"PB": 1024, "TB": 1, "GB": 1 / 1024, "MB": 1 / 1024 ** 2, "KB": 1 / 1024 ** 3, "B": 1 / 1024 ** 4}
        return num * factor.get(unit, 1)

    # ====================== 通知 ======================
    def _notify_msg(self, title, text):
        logger.info(f"[{title}] 通知内容:\n{text}")
        if not self._notify:
            return
        sent = False
        try:
            self.post_message(title=title, text=text)
            sent = True
        except Exception as e:
            logger.warn(f"ZMPT post_message 失败，回退 eventmanager: {e}")
        if not sent:
            try:
                self.eventmanager.send_event(EventType.Notification, {
                    "channel": None, "title": title, "text": text, "image": "", "userid": None,
                })
            except Exception as e:
                logger.error(f"ZMPT 通知发送失败: {e}")
