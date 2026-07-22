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
    plugin_version = "1.2.5"
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
    _stats = {}  # 不合格次数累计 {uid: {name, group, count}}，每月1号7点重置
    _browser_install_attempted = False  # 本次会话是否已尝试过自动安装浏览器内核

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
        """定时任务：① 按用户 cron 检查；② 每月1号7点发送不合格统计并重置"""
        if not self.get_state():
            return []
        services = []
        if self._cron:
            try:
                trigger = CronTrigger.from_crontab(self._cron)
            except Exception as e:
                logger.error(f"ZMPT cron 表达式非法: {self._cron} -> {e}")
            else:
                services.append({
                    "id": "ZmptKeeperCheck.Check",
                    "name": "ZMPT保种组检查",
                    "trigger": trigger,
                    "func": self.check,
                    "kwargs": {},
                })
        # 每月1号 07:00 发送累计不合格统计并重置
        try:
            services.append({
                "id": "ZmptKeeperCheck.Monthly",
                "name": "ZMPT月度统计与重置",
                "trigger": CronTrigger.from_crontab("0 7 1 * *"),
                "func": self.monthly_report,
                "kwargs": {},
            })
        except Exception as e:
            logger.error(f"ZMPT 月度 cron 非法: {e}")
        return services

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
                            "text": "打开\"立即运行一次\"开关并保存，3秒后执行一次检查（执行完会自动关闭）。组配置已内置（5T组id=6 / 10T组id=10），无需填写。每次检查会累计不合格次数，每月1号7点推送统计并重置，插件详情页可查看。"}},
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
        stats = self._load_stats()
        result = []
        # 顶部说明
        result.append({"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 12}, "content": [
                {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                    "text": "📊 不合格累计统计（每月1号 07:00 推送并重置）。点「复制」可粘到电子表格。"}},
            ]},
        ]})
        if not stats:
            result.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                        "text": "暂无统计。运行一次检查后会开始累计不合格次数。"}},
                ]},
            ]})
        else:
            by_group = {}
            for uid, info in stats.items():
                g = (info.get("group") or "未知组").strip()
                by_group.setdefault(g, []).append((str(uid), (info.get("name") or "").strip(), int(info.get("count", 0) or 0)))
            for g in ["5T组", "10T组"] + [k for k in by_group if k not in ("5T组", "10T组")]:
                members = sorted(by_group.pop(g, []), key=lambda x: (-x[2], x[0]))
                if not members:
                    continue
                result.append(self._stats_group_card(g, members))
        # 底部：最近一次检查结果
        result.append({"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 12}, "content": [
                {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                    "text": self._last_result or "尚未运行。"}},
            ]},
        ]})
        return result

    def _stats_group_card(self, group_name, members):
        """构造一个组的不合格统计卡片：组名 + 复制按钮 + HTML 表格（ID/用户名/不合格次数）。"""
        tsv = "ID\t用户名\t不合格次数\n" + "\n".join(f"{uid}\t{name}\t{cnt}" for uid, name, cnt in members)
        tsv_js = json.dumps(tsv)
        onclick = ("(e) => { try { var t = " + tsv_js + "; "
                   "if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(t); } "
                   "else { var ta=document.createElement('textarea'); ta.value=t; document.body.appendChild(ta); "
                   "ta.select(); document.execCommand('copy'); document.body.removeChild(ta); } } catch(err){} }")
        rows = ""
        for idx, (uid, name, cnt) in enumerate(members):
            bg = "background:#f7f9fc;" if idx % 2 else "background:#ffffff;"
            rows += (
                f"<tr style='{bg}'>"
                f"<td style='border:1px solid #d0d7de;padding:5px 10px'>{uid}</td>"
                f"<td style='border:1px solid #d0d7de;padding:5px 10px'>{name}</td>"
                f"<td style='border:1px solid #d0d7de;padding:5px 10px;text-align:center;font-weight:600'>{cnt}</td>"
                f"</tr>"
            )
        table_html = (
            "<table style='border-collapse:collapse;width:100%;font-size:13px;margin-top:6px'>"
            "<thead><tr style='background:#eef1f5'>"
            "<th style='border:1px solid #d0d7de;padding:6px 10px;text-align:left'>ID</th>"
            "<th style='border:1px solid #d0d7de;padding:6px 10px;text-align:left'>用户名</th>"
            "<th style='border:1px solid #d0d7de;padding:6px 10px;text-align:center'>不合格次数</th>"
            "</tr></thead><tbody>" + rows + "</tbody></table>"
        )
        return {"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 12}, "content": [
                {"component": "VCard", "props": {"variant": "outlined", "class": "mb-3"}, "content": [
                    {"component": "VRow", "props": {"align": "center"}, "content": [
                        {"component": "VCol", "content": [
                            {"component": "div", "html": f"<div style='font-weight:700;font-size:15px;padding:4px 8px'>{group_name} · 不合格 {len(members)} 人</div>"},
                        ]},
                        {"component": "VCol", "props": {"cols": "auto"}, "content": [
                            {"component": "VBtn", "props": {"size": "small", "color": "primary", "variant": "outlined",
                                "onclick": onclick}, "text": "📋 复制本组"},
                        ]},
                    ]},
                    {"component": "div", "html": table_html},
                ]},
            ]},
        ]}

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
        self._stats = self._load_stats()
        summary = []
        for g in self._groups:
            try:
                summary.append(self._check_group(g))
            except Exception as e:
                logger.error(f"ZMPT保种组检查 组{g.get('id')} 出错: {e}")
                logger.error(traceback.format_exc())
                summary.append(f"{g.get('name', '组' + str(g.get('id')))}：执行出错 {e}")
            # 组之间多等一会 + 强制回收，避免前一组浏览器进程未释放导致后一组起不来
            try:
                import gc
                gc.collect()
            except Exception:
                pass
            time.sleep(15)
        self._save_stats(self._stats)
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
                    self._bump_stat(g["name"], u)
            rows.append({"id": u["id"], "name": u["name"], "level": u["level"],
                         "vol": volstr, "intt": intt, "status": status})
            time.sleep(self._delay)
        text = self._format_text(g, rows, ok_n, bad_n, err_n)
        self._notify_msg(f"ZMPT {g['name']} 审查结果", text)
        summary = f"【{g['name']}】共 {len(rows)} 人 · 合格 {ok_n} / 不合格 {bad_n} / 异常 {err_n}"
        return summary

    def _format_text(self, g, rows, ok_n, bad_n, err_n):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        bad_rows = [r for r in rows if r["status"] == "不合格"]
        lines = [
            f"抓取时间：{now}",
            f"{g['name']} · 阈值 ≥ {g['threshold']:.0f} TB · 共 {len(rows)} 人（合格 {ok_n} / 不合格 {bad_n} / 异常 {err_n}）",
        ]
        if bad_rows:
            lines.append("")
            lines.append(f"不合格组员（{len(bad_rows)} 人）：")
            lines.append("ID\t用户名\t官种体积")
            for r in bad_rows:
                lines.append(f"{r['id']}\t{r['name']}\t{r['vol']}")
        else:
            lines.append("✅ 全部合格，无不合格组员。")
        return "\n".join(lines)

    # ====================== 不合格次数统计（持久化，每月1号7点重置）======================
    def _load_stats(self):
        try:
            d = self.get_data("unqual_stats")
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}

    def _save_stats(self, stats):
        try:
            self.save_data("unqual_stats", stats or {})
        except Exception as e:
            logger.warn(f"ZMPT 保存不合格统计失败: {e}")

    def _bump_stat(self, group_name, u):
        """某组员本次判定为不合格，累计 +1。"""
        uid = str(u.get("id") or "").strip()
        if not uid:
            return
        s = self._stats.get(uid) or {"name": "", "group": group_name, "count": 0}
        s["name"] = (u.get("name") or "").strip() or s.get("name") or f"id:{uid}"
        s["group"] = group_name
        s["count"] = int(s.get("count", 0) or 0) + 1
        self._stats[uid] = s

    def _format_stats(self, stats=None):
        """把累计统计格式化成文本，按组分类，按次数降序。"""
        stats = self._load_stats() if stats is None else (stats or {})
        by_group = {}
        for uid, info in stats.items():
            g = (info.get("group") or "未知组").strip()
            by_group.setdefault(g, []).append((str(uid), (info.get("name") or "").strip(), int(info.get("count", 0) or 0)))
        lines = []
        for g in ["5T组", "10T组"]:
            members = sorted(by_group.pop(g, []), key=lambda x: (-x[2], x[0]))
            if not members:
                continue
            lines.append(f"【{g} 不合格统计】共 {len(members)} 人（自上次重置累计）")
            lines.append("ID\t用户名\t不合格次数")
            for uid, name, cnt in members:
                lines.append(f"{uid}\t{name}\t{cnt}")
            lines.append("")
        # 其余未识别的组
        for g, members in by_group.items():
            members.sort(key=lambda x: (-x[2], x[0]))
            lines.append(f"【{g} 不合格统计】共 {len(members)} 人")
            lines.append("ID\t用户名\t不合格次数")
            for uid, name, cnt in members:
                lines.append(f"{uid}\t{name}\t{cnt}")
            lines.append("")
        return "\n".join(lines).strip() if lines else "（暂无不合格记录）"

    def monthly_report(self):
        """每月1号7点：发送累计不合格统计，然后重置。"""
        logger.info("ZMPT保种组检查：执行月度统计与重置")
        stats = self._load_stats()
        text = self._format_stats(stats)
        total = sum(int(v.get("count", 0) or 0) for v in stats.values())
        title = f"ZMPT 月度不合格统计（{datetime.now().strftime('%Y-%m')}，{len(stats)}人/{total}次，即将重置）"
        self._notify_msg(title, text if stats else "本月无不合格记录。")
        self._save_stats({})
        self._stats = {}

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

    # 移植自油猴脚本 zmpt-官种体积检查.user.js：设每页100 + 点“下一页”翻页，返回组员列表 [{id,name,href,level}]
    _FETCH_ALL_JS = r"""
async () => {
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const textOf = (n) => { if (!n) return ''; return (n.textContent || n.innerText || '').trim(); };
  const sig = () => {
    const rows = document.querySelectorAll('table tbody tr, table tr');
    let s = rows.length + '|', n = 0;
    for (const r of rows) { if (n >= 3) break; s += textOf(r).replace(/\s+/g, '').slice(0, 24) + '#'; n++; }
    return s;
  };
  const waitStable = async (prev, timeout) => {
    timeout = timeout || 8000;
    const t0 = Date.now();
    while (Date.now() - t0 < timeout) { await sleep(150); if (sig() !== prev) break; }
    let last = sig(), since = Date.now();
    while (Date.now() - t0 < timeout) {
      await sleep(200);
      const cur = sig();
      if (cur === last) { if (Date.now() - since > 600) return cur; } else { last = cur; since = Date.now(); }
    }
    return sig();
  };
  // 触发 Filament 表格懒加载：反复 loadTable + scrollIntoView，直到出现组员链接
  for (let i = 0; i < 20; i++) {
    if (document.querySelectorAll('a[href*="userdetails"]').length > 0) break;
    try {
      const L = window.Livewire;
      if (L && L.find) {
        const els = document.getElementsByTagName('*');
        for (const el of els) {
          const id = el.getAttribute && el.getAttribute('wire:id');
          if (id) {
            try { el.scrollIntoView({ block: 'center' }); } catch (e) {}
            const c = L.find(id);
            if (c) ['loadTable', 'loadRecords'].forEach(m => { try { c.call(m); } catch (e) {} });
          }
        }
      }
    } catch (e) {}
    await sleep(1000);
  }
  // 设每页条数（按 option 值识别 select，和油猴脚本一致）
  const findPerPageSelect = () => {
    for (const s of document.querySelectorAll('select')) {
      const vals = Array.from(s.options).map(o => parseInt(o.value)).filter(v => !isNaN(v));
      if (vals.length >= 2 && vals.every(v => v <= 500) && (vals.includes(10) || vals.includes(25) || vals.includes(50))) return s;
    }
    return null;
  };
  const setPerPage = async (target) => {
    const sel = findPerPageSelect();
    if (!sel) return false;
    const opt = Array.from(sel.options).find(o => parseInt(o.value) === target)
      || Array.from(sel.options).find(o => parseInt(o.value) >= target)
      || Array.from(sel.options).reduce((a, b) => (parseInt(a.value) > parseInt(b.value) ? a : b));
    if (parseInt(sel.value) === parseInt(opt.value)) return true;
    const before = sig();
    sel.value = opt.value;
    sel.dispatchEvent(new Event('input', { bubbles: true }));
    sel.dispatchEvent(new Event('change', { bubbles: true }));
    await waitStable(before);
    return parseInt(sel.value) === parseInt(opt.value);
  };
  await setPerPage(100).catch(() => {});
  // 找“下一页”按钮（按 aria-label / 文本，和油猴脚本一致）
  const findNext = () => {
    const all = Array.from(document.querySelectorAll('a, button, [role="button"], [dusk]'));
    for (const el of all) {
      const aria = (el.getAttribute('aria-label') || '').toLowerCase();
      if (aria === 'next' || aria === '下一页' || aria.includes('next page') || aria.includes('pagination.next')) return el;
    }
    for (const el of all) {
      const t = textOf(el).toLowerCase();
      if (['下一页', 'next', '>', '›', '»', '→'].includes(t)) return el;
    }
    return null;
  };
  const isDisabled = (el) => {
    if (!el) return true;
    if (el.disabled) return true;
    if (el.getAttribute('aria-disabled') === 'true') return true;
    if (/(^|\s)disabled(\s|$)/i.test((el.className || '').toString())) return true;
    return false;
  };
  // 解析当前页组员（userdetails 链接 + 等级列）
  const parseUsers = () => {
    const users = [], seen = new Set();
    const add = (id, name, href, level) => {
      const k = id || href;
      if (!k || seen.has(k)) return;
      seen.add(k);
      users.push({ id: String(id || ''), name: name || (id ? ('id:' + id) : ''), href: href || (id ? ('https://zmpt.cc/userdetails.php?id=' + id) : ''), level: level || '' });
    };
    document.querySelectorAll('a[href*="userdetails"]').forEach(a => {
      const raw = a.getAttribute('href') || '';
      let href = raw; try { href = new URL(raw, location.href).href; } catch (e) {}
      const m = raw.match(/[?&](?:id|userid|uid)=(\d+)/);
      const id = m ? m[1] : '';
      const name = textOf(a) || (id ? ('id:' + id) : '');
      let level = '';
      const tr = a.closest('tr');
      if (tr) {
        const table = tr.closest('table');
        if (table) {
          const head = table.querySelector('tr');
          let col = -1;
          if (head) Array.from(head.querySelectorAll('th, td')).forEach((h, i) => { if (col < 0 && /等级|用户组|class|level|group/i.test(textOf(h))) col = i; });
          if (col >= 0) { const cells = tr.querySelectorAll('td'); level = cells[col] ? textOf(cells[col]) : ''; }
        }
      }
      add(id, name, href, level);
    });
    return users;
  };
  // 翻页收集（这一页没有新组员就停，避免无效翻页导致超时）
  const all = []; const seenAll = new Set(); let safety = 0;
  try {
    while (safety++ < 30) {
      const pageUsers = parseUsers();
      if (pageUsers.length === 0) break;
      let added = 0;
      for (const u of pageUsers) { const k = u.id || u.href; if (!seenAll.has(k)) { seenAll.add(k); all.push(u); added++; } }
      if (added === 0) break;  // 没有新组员 → 到最后一页了（或翻页没生效），停
      const nextBtn = findNext();
      if (!nextBtn || isDisabled(nextBtn)) break;
      const before = sig();
      nextBtn.click();
      await waitStable(before);
    }
  } catch (e) {}
  return all;
}
"""

    def _fetch_users_browser(self, role_id):
        """浏览器模式：渲染组员页，跑移植自油猴脚本的“设每页100+翻页”逻辑，返回 (users, diag)。"""
        url = (self._member_url.replace("{id}", str(role_id)).replace("{page}", "1")
               if self._member_url else f"{self._base}/nexusphp/roles/{role_id}/edit?page=1")
        raw, err = self._render_with_browser(url)
        if not raw:
            return [], f"[浏览器模式] 内置浏览器渲染失败：{err} | URL={url}"
        users = []
        for u in raw:
            uid = str(u.get("id") or "").strip()
            if not uid:
                continue
            users.append({
                "id": uid,
                "name": (u.get("name") or f"id:{uid}").strip(),
                "level": (u.get("level") or "").strip(),
                "href": (u.get("href") or f"{self._base}/userdetails.php?id={uid}").strip(),
            })
        if not users:
            return [], f"[浏览器模式] 表格已加载但未解析到组员链接 | URL={url}"
        return users, None

    def _ensure_browser(self):
        """首次使用浏览器模式时自动准备 Chromium 内核（快速探测；缺失则自动安装，只做一次）。"""
        if self.get_data("browser_prepared") or self._browser_install_attempted:
            return
        self._browser_install_attempted = True
        # 1) 先快速探测 MP 浏览器能不能用（开个 about:blank）
        try:
            from app.helper.browser import PlaywrightHelper
            if PlaywrightHelper().get_page_source("about:blank", headless=True, timeout=30):
                self.save_data("browser_prepared", True)
                logger.info("ZMPT：浏览器内核可用，无需安装。")
                return
        except Exception as e:
            logger.warn(f"ZMPT：浏览器内核探测失败，将尝试自动安装: {e}")
        # 2) 探测失败 → 自动安装 Chromium 内核（仅内核，不动系统库）
        logger.info("ZMPT：开始自动安装 Chromium 内核（约150MB，只需一次）")
        self._notify_msg("ZMPT 浏览器内核",
                         "首次使用：检测到浏览器内核缺失，正在自动安装 Chromium（约150MB，只需一次），请耐心等待几分钟…")
        try:
            import subprocess
            import sys
            r = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                               capture_output=True, text=True, timeout=600)
            tail = (((r.stdout or "") + "\n" + (r.stderr or ""))[-400:]).strip()
            if r.returncode == 0:
                self.save_data("browser_prepared", True)
                logger.info("ZMPT：Chromium 内核安装完成")
                self._notify_msg("ZMPT 浏览器内核", "✅ Chromium 内核已安装，开始抓取。")
            else:
                logger.warn(f"ZMPT Chromium 安装失败 rc={r.returncode}: {tail}")
                self._notify_msg("ZMPT 浏览器内核",
                                 f"⚠️ 自动安装失败：{tail[:200]}\n建议改用官方 MoviePilot 镜像（自带浏览器内核）。")
        except Exception as e:
            logger.warn(f"ZMPT 自动安装浏览器内核异常: {e}")
            self._notify_msg("ZMPT 浏览器内核",
                             f"⚠️ 无法自动安装浏览器内核：{e}\n建议改用官方 MoviePilot 镜像（自带浏览器内核）。")

    def _render_with_browser(self, url):
        """用 MP 内置浏览器渲染组员页。返回 (data, error)：data 为组员列表或 None，error 为失败原因。"""
        self._ensure_browser()
        try:
            from app.helper.browser import PlaywrightHelper
        except Exception as e:
            logger.warn(f"ZMPT PlaywrightHelper 不可用: {e}")
            return None, f"PlaywrightHelper 导入失败({e})——你的 MP 可能未集成浏览器模块"

        last_err = [""]  # 回调内捕获的错误（用 list 做可变持有）

        def _collect(page):
            try:
                page.set_default_timeout(20000)
            except Exception:
                pass
            # 1) 把 Cookie 写进 cookie jar 再重新加载，保证 Livewire/Filament AJAX 认证通过
            try:
                jar = [{"name": k, "value": v, "domain": "zmpt.cc", "path": "/"}
                       for k, v in self._cookie_dict().items()]
                if jar:
                    page.context.add_cookies(jar)
            except Exception as e:
                logger.warn(f"ZMPT 写入浏览器cookie失败: {e}")
            try:
                page.goto(url)
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            # 2) 轮询触发表格加载：反复 loadTable + scrollIntoView，直到出现组员链接或超时(~30s)
            for _ in range(15):
                try:
                    page.evaluate("""() => {
                        try {
                            const L = window.Livewire;
                            if (L && L.find) {
                                const els = document.getElementsByTagName('*');
                                for (const el of els) {
                                    const id = el.getAttribute && el.getAttribute('wire:id');
                                    if (id) {
                                        try { el.scrollIntoView({block: 'center'}); } catch(e){}
                                        const comp = L.find(id);
                                        if (comp) ['loadTable','loadRecords'].forEach(m => { try { comp.call(m); } catch(e){} });
                                    }
                                }
                            }
                        } catch(e){}
                    }""")
                except Exception:
                    pass
                try:
                    page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    pass
                try:
                    n = page.evaluate("""() => {
                        let n = 0;
                        document.querySelectorAll('a[href]').forEach(a => {
                            const h = a.href || '';
                            if (/(user|profile|member|userdetails|uid|userid)/i.test(h) && /[0-9]{3,}/.test(h)) n++;
                        });
                        return n;
                    }""")
                except Exception:
                    n = 0
                if n and n > 0:
                    break
                try:
                    page.evaluate("() => new Promise(r => setTimeout(r, 1500))")
                except Exception:
                    pass
            # 3) 运行移植自油猴脚本的抓取逻辑：设每页100 + 点"下一页"翻页，直接返回组员列表
            try:
                return page.evaluate(self._FETCH_ALL_JS)
            except Exception as e:
                last_err[0] = f"抓取脚本执行失败: {e}"
                logger.warn(f"ZMPT 浏览器抓取脚本执行失败: {e}")
                return None

        for attempt in range(4):
            try:
                html = PlaywrightHelper().action(url, _collect,
                                                 cookies=self._cookie, headless=True, timeout=240)
                if html:
                    return html, ""
            except Exception as e:
                last_err[0] = f"浏览器渲染异常: {e}"
                logger.warn(f"ZMPT 浏览器渲染失败(第{attempt+1}次): {e}")
            try:
                import gc
                gc.collect()
            except Exception:
                pass
            time.sleep(10)
        err = last_err[0] or ("浏览器启动或页面加载失败——常见原因：MP 未安装/未启用 Playwright 浏览器内核、内存不足、容器沙箱拦截。"
                              "请到 MP 日志搜索 '网页操作失败' / 'CloakBrowser' / 'playwright' 查看具体报错。")
        return None, err

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
