import re
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Tuple

from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup

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
    plugin_version = "1.0.1"
    plugin_author = "2536003090"
    author_url = "https://github.com/2536003090"
    plugin_config_prefix = "zmptkeeper_"
    plugin_order = 28
    auth_level = 1

    # ===== 运行时状态 =====
    _enabled = False
    _cookie = ""
    _base = "https://zmpt.cc"
    _cron = "0 8 * * *"
    _notify = True
    _delay = 1.0
    _groups = []
    _last_result = ""
    _member_url = ""  # 组员列表URL模板，支持 {id} 和 {page} 占位符；空则用内置默认

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._cookie = (config.get("cookie") or "").strip()
        self._cron = (config.get("cron") or "0 8 * * *").strip()
        self._notify = config.get("notify") is not False
        try:
            self._delay = float(config.get("delay") or 1.0)
        except Exception:
            self._delay = 1.0
        self._groups = self._parse_groups_config(config.get("groups"))
        self._member_url = (config.get("member_url") or "").strip()
        logger.info(f"ZMPT保种组检查 已加载：enabled={self._enabled} cron={self._cron} groups={self._groups} member_url={self._member_url or '(默认)'}")

    @staticmethod
    def _parse_groups_config(raw):
        """raw: id:名称:阈值T，多组用分号分隔。空则用默认两组。"""
        if not raw:
            return [
                {"id": "6", "name": "5T组", "threshold": 5.0},
                {"id": "10", "name": "10T组", "threshold": 10.0},
            ]
        result = []
        for part in str(raw).split(";"):
            part = part.strip()
            if not part:
                continue
            seg = part.split(":")
            if len(seg) < 3:
                continue
            try:
                result.append({
                    "id": seg[0].strip(),
                    "name": seg[1].strip(),
                    "threshold": float(seg[2]),
                })
            except Exception:
                continue
        return result

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
        # 无额外后台线程；定时任务由框架统一管理
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页：返回 (页面JSON, 默认配置)。model 必须放在 props 内。"""
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}},
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSwitch", "props": {"model": "notify", "label": "推送通知"}},
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
                        {"component": "VTextField", "props": {"model": "cron",
                            "label": "定时 cron（5字段：分 时 日 月 周）", "placeholder": "0 8 * * *（每天8点）"}},
                    ]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {"model": "delay", "label": "请求间隔(秒)", "placeholder": "1.0"}},
                    ]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextField", "props": {"model": "groups",
                            "label": "组配置（id:名称:阈值T，分号分隔）",
                            "placeholder": "6:5T组:5;10:10T组:10"}},
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
                            "text": "若提示\"未抓到组员\"：用浏览器打开保种组组员列表页，把地址栏网址粘到上面的\"组员列表URL模板\"（数字换成 {id}，加 &page={page}）。保存后发送 /zmpt_check 立即执行一次。"}},
                    ]},
                ]},
            ]},
        ], {
            "enabled": False,
            "notify": True,
            "cookie": "",
            "cron": "0 8 * * *",
            "delay": 1.0,
            "groups": "6:5T组:5;10:10T组:10",
            "member_url": "",
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
            msg = f"⚠️ {g['name']}：未抓到组员（组id={g['id']}）。\n诊断：{diag}"
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
        summary = f"【{g['name']}】共 {len(rows)} 人 · 合格 {ok_n} / 不合格 {bad_n} / 异常 {err_n}"
        return summary

    def _format_text(self, g, rows, ok_n, bad_n, err_n):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [
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

    def _build_diag(self, url, text, status, length):
        """构造可读的诊断串，帮助定位 抓不到组员 的原因。"""
        if status is None:
            return f"请求失败/超时（网络异常或请求被拦截）| URL={url}"
        parts = [f"HTTP {status}", f"长度 {length}"]
        if text:
            low = text.lower()
            if (any(k in text for k in ("登录", "请先登录", "您还没有登录", "还未登录", "userlogin", "takelogin"))
                    or "login.php" in low or "takelogin" in low):
                parts.append("疑似登录页（Cookie 可能已失效）")
            snippet = re.sub(r"\s+", " ", text).strip()[:160]
            parts.append(f"片段: {snippet}")
        elif status == 200:
            parts.append("HTTP 200 但响应为空")
        return " | ".join(parts) + f" | URL={url}"

    # ====================== 抓组员 ======================
    def _fetch_users(self, role_id):
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
            page_users = self._parse_users(html)
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

    def _parse_users(self, html):
        soup = BeautifulSoup(html, "html.parser")
        users = []
        for a in soup.select('a[href*="userdetails"]'):
            href = a.get("href", "")
            m = re.search(r'[?&](?:id|userid|uid)=(\d+)', href)
            if not m:
                continue
            uid = m.group(1)
            name = a.get_text(strip=True) or f"id:{uid}"
            tr = a.find_parent("tr")
            table = tr.find_parent("table") if tr else None
            level = self._level_from_tr(table, tr) if table else ""
            users.append({"id": uid, "name": name, "level": level, "href": href})
        return users

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
