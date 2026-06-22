"""
DingTalk Approval Adapter — Hermes Gateway plugin.

Inherits the built-in DingTalkAdapter and adds interactive AI Card approval
prompts for dangerous command confirmation.

4 buttons:
  ✅ 仅此次 (approve_once)
  ✅ 本次会话 (approve_session)
  ✅ 永久允许 (approve_always)
  ❌ 拒绝 (deny)

Registered as the "dingtalk" platform, overriding the built-in adapter
(platform_registry is last-writer-wins). No official source files are modified.

Configuration (config.yaml):
    platforms:
      dingtalk:
        enabled: true
        extra:
          approval_template_id: "<your-template-id>.schema"
          # allowed_approvers: "userid1,userid2"  # optional, comma-separated

Card template variables:
  ${title}        — card header title
  ${content}      — markdown body (command + reason)
  ${status}       — "pending" → shows buttons; anything else → hides them
  ${result_label} — shown after click (e.g. "✅ 已批准（仅此次）  22:32")

Button callbacks carry hermes_action in cardPrivateData.params:
  approve_once / approve_session / approve_always / deny
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ── Optional SDK imports ───────────────────────────────────────────────────

try:
    import dingtalk_stream
    DINGTALK_STREAM_AVAILABLE = True
except ImportError:
    DINGTALK_STREAM_AVAILABLE = False
    dingtalk_stream = None  # type: ignore[assignment]

try:
    from alibabacloud_dingtalk.card_1_0 import (
        client as dingtalk_card_client,
        models as dingtalk_card_models,
    )
    from alibabacloud_tea_openapi import models as open_api_models
    from alibabacloud_tea_util import models as tea_util_models
    CARD_SDK_AVAILABLE = True
except ImportError:
    CARD_SDK_AVAILABLE = False
    dingtalk_card_client = None  # type: ignore[assignment]
    dingtalk_card_models = None  # type: ignore[assignment]
    open_api_models = None  # type: ignore[assignment]
    tea_util_models = None  # type: ignore[assignment]

# ── Constants ──────────────────────────────────────────────────────────────

_ACTION_TO_CHOICE: Dict[str, str] = {
    "approve_once": "once",
    "approve_session": "session",
    "approve_always": "always",
    "deny": "deny",
}

_CHOICE_LABEL: Dict[str, str] = {
    "once": "✅ 已批准（仅此次）",
    "session": "✅ 已批准（本次会话）",
    "always": "✅ 已永久批准",
    "deny": "❌ 已拒绝",
}

# In-process state: out_track_id → {session_key, chat_id, sender_staff_id, allowed_approvers, title}
_PENDING_APPROVALS: Dict[str, Dict[str, str]] = {}

# Card callback STREAM topic
_CARD_CALLBACK_TOPIC = "/v1.0/card/instances/callback"


# ── Card callback handler ──────────────────────────────────────────────────

def _build_card_callback_handler_class() -> type:
    """Return a CallbackHandler subclass (or plain object when SDK unavailable)."""
    bases: tuple
    if DINGTALK_STREAM_AVAILABLE:
        try:
            from dingtalk_stream import CallbackHandler  # type: ignore[attr-defined]
            bases = (CallbackHandler,)
        except Exception:
            bases = (object,)
    else:
        bases = (object,)

    class _CardCallbackHandler(*bases):  # type: ignore[misc]
        TOPIC = _CARD_CALLBACK_TOPIC

        def __init__(self, adapter: Any, loop: asyncio.AbstractEventLoop) -> None:
            if bases[0] is not object:
                bases[0].__init__(self)  # type: ignore[call-arg]
            self._adapter = adapter
            self._loop = loop

        async def process(self, callback: Any) -> tuple:  # type: ignore[override]
            """Called by dingtalk-stream when a card button is clicked."""
            try:
                data = getattr(callback, "data", callback)
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except Exception:
                        data = {}

                if not isinstance(data, dict):
                    return 200, "ok"

                out_track_id = data.get("outTrackId", "")

                content = data.get("content", {})
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except Exception:
                        content = {}
                params: Dict[str, str] = {}
                if isinstance(content, dict):
                    params = (
                        content.get("cardPrivateData", {}).get("params", {}) or {}
                    )

                hermes_action = params.get("hermes_action", "")
                clicker_id = data.get("userId", "") or data.get("staffId", "")

                logger.info(
                    "[dingtalk-approval] card callback out_track_id=%s action=%s clicker=%s",
                    out_track_id, hermes_action, clicker_id,
                )

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    self._handle_click,
                    out_track_id,
                    hermes_action,
                    clicker_id,
                )
            except Exception:
                logger.exception("[dingtalk-approval] Error in card callback")
            return 200, "ok"

        def _handle_click(self, out_track_id: str, action: str, clicker_id: str) -> None:
            state = _PENDING_APPROVALS.get(out_track_id)
            if not state:
                logger.warning("[dingtalk-approval] Unknown/expired out_track_id: %s", out_track_id)
                return

            session_key = state.get("session_key", "")
            allowed_approvers = state.get("allowed_approvers", "")

            if allowed_approvers:
                allowed_set = {u.strip() for u in allowed_approvers.split(",") if u.strip()}
                if "*" not in allowed_set and clicker_id not in allowed_set:
                    logger.warning(
                        "[dingtalk-approval] Unauthorized click by %s (session=%s)",
                        clicker_id, session_key,
                    )
                    return

            choice = _ACTION_TO_CHOICE.get(action)
            if not choice:
                logger.warning("[dingtalk-approval] Unknown action: %s", action)
                return

            try:
                from tools.approval import resolve_gateway_approval, has_blocking_approval
                if not has_blocking_approval(session_key):
                    logger.info("[dingtalk-approval] Already resolved: %s", session_key)
                    return
                count = resolve_gateway_approval(session_key, choice)
                logger.info(
                    "[dingtalk-approval] Resolved %d approval(s) session=%s choice=%s user=%s",
                    count, session_key, choice, clicker_id,
                )
            except Exception:
                logger.exception("[dingtalk-approval] resolve_gateway_approval failed")
                return

            label = _CHOICE_LABEL.get(choice, choice)
            asyncio.run_coroutine_threadsafe(
                self._adapter._update_approval_card(out_track_id, label),
                self._loop,
            )
            _PENDING_APPROVALS.pop(out_track_id, None)

    return _CardCallbackHandler


# ── Adapter subclass ───────────────────────────────────────────────────────

def _get_base_dingtalk_adapter():
    """Return the DingTalk adapter from either the new plugin runtime or old core path."""
    try:
        from hermes_plugins.dingtalk_platform.adapter import (  # type: ignore[import-not-found]
            DingTalkAdapter,
            check_dingtalk_requirements,
        )
        return DingTalkAdapter, check_dingtalk_requirements
    except Exception:
        pass
    try:
        from plugins.platforms.dingtalk.adapter import (  # type: ignore[import-not-found]
            DingTalkAdapter,
            check_dingtalk_requirements,
        )
        return DingTalkAdapter, check_dingtalk_requirements
    except Exception:
        pass
    from gateway.platforms.dingtalk import (  # type: ignore[import-not-found]
        DingTalkAdapter,
        check_dingtalk_requirements,
    )
    return DingTalkAdapter, check_dingtalk_requirements

def _build_adapter_class() -> type:
    DingTalkAdapter, _ = _get_base_dingtalk_adapter()
    from gateway.platforms.base import SendResult

    CardCallbackHandlerClass = _build_card_callback_handler_class()

    class _DingTalkApprovalAdapter(DingTalkAdapter):
        """DingTalkAdapter extended with interactive 4-button approval cards."""

        # ── helpers ────────────────────────────────────────────────────────

        @property
        def _approval_template_id(self) -> Optional[str]:
            extra = self.config.extra or {}
            return (
                extra.get("approval_template_id")
                or os.getenv("DINGTALK_APPROVAL_TEMPLATE_ID")
                or extra.get("card_template_id")
                or os.getenv("DINGTALK_CARD_TEMPLATE_ID")
            )

        @property
        def _allowed_approvers_str(self) -> str:
            extra = self.config.extra or {}
            return (
                extra.get("allowed_approvers")
                or os.getenv("DINGTALK_ALLOWED_APPROVERS", "")
                or ""
            )

        # ── connect() — also register card callback handler ────────────────

        async def connect(self) -> bool:
            ok = await super().connect()
            if not ok:
                return False

            # Init card SDK when only approval_template_id is set
            # (parent only inits _card_sdk when card_template_id exists)
            if CARD_SDK_AVAILABLE and self._approval_template_id and not self._card_sdk:
                try:
                    sdk_cfg = open_api_models.Config()
                    sdk_cfg.protocol = "https"
                    sdk_cfg.region_id = "central"
                    self._card_sdk = dingtalk_card_client.Client(sdk_cfg)
                    logger.info(
                        "[dingtalk-approval] Card SDK initialised for approval template: %s",
                        self._approval_template_id,
                    )
                except Exception as e:
                    logger.warning("[dingtalk-approval] Could not init card SDK: %s", e)

            # Register card callback handler on the same stream client
            if DINGTALK_STREAM_AVAILABLE and self._stream_client:
                try:
                    loop = asyncio.get_running_loop()
                    handler = CardCallbackHandlerClass(self, loop)
                    self._stream_client.register_callback_handler(
                        _CARD_CALLBACK_TOPIC, handler
                    )
                    logger.info(
                        "[dingtalk-approval] Card callback handler registered (topic=%s)",
                        _CARD_CALLBACK_TOPIC,
                    )
                except Exception as e:
                    logger.warning(
                        "[dingtalk-approval] Could not register card callback handler: %s", e
                    )

            return True

        # ── send_exec_approval() ────────────────────────────────────────────

        async def send_exec_approval(
            self,
            chat_id: str,
            command: str,
            session_key: str,
            description: str = "dangerous command",
            metadata: Optional[Dict[str, Any]] = None,
        ) -> "SendResult":
            """Send an AI Card with 4 approval buttons."""
            tmpl = self._approval_template_id
            if not tmpl:
                return SendResult(
                    success=False,
                    error=(
                        "No approval_template_id configured. "
                        "Add platforms.dingtalk.extra.approval_template_id in config.yaml."
                    ),
                )

            if not CARD_SDK_AVAILABLE:
                return SendResult(success=False, error="alibabacloud-dingtalk SDK not installed")

            if not self._card_sdk:
                return SendResult(success=False, error="Card SDK not initialised")

            token = await self._get_access_token()
            if not token:
                return SendResult(success=False, error="Could not obtain DingTalk access token")

            message_ctx = self._message_contexts.get(chat_id)
            conversation_id = getattr(message_ctx, "conversation_id", "") or chat_id
            conversation_type = getattr(message_ctx, "conversation_type", "1")
            is_group = str(conversation_type) == "2"
            sender_staff_id = getattr(message_ctx, "sender_staff_id", "") or ""

            cmd_preview = command[:500] + "\n…（已截断）" if len(command) > 500 else command
            content_md = (
                f"**命令预览：**\n\n```\n{cmd_preview}\n```\n\n"
                f"**原因：** {description}"
            )

            out_track_id = f"hermes-approval-{uuid.uuid4().hex[:12]}"

            card_param_map = {
                "title": "⚠️ 命令审批请求",
                "content": content_md,
                "status": "pending",
                "result_label": "",
            }

            try:
                runtime = tea_util_models.RuntimeOptions()

                # Step 1: Create card instance
                create_req = dingtalk_card_models.CreateCardRequest(
                    card_template_id=tmpl,
                    out_track_id=out_track_id,
                    card_data=dingtalk_card_models.CreateCardRequestCardData(
                        card_param_map=card_param_map,
                    ),
                    callback_type="STREAM",
                    im_group_open_space_model=(
                        dingtalk_card_models.CreateCardRequestImGroupOpenSpaceModel(
                            support_forward=False,
                        )
                    ),
                    im_robot_open_space_model=(
                        dingtalk_card_models.CreateCardRequestImRobotOpenSpaceModel(
                            support_forward=False,
                        )
                    ),
                )
                create_headers = dingtalk_card_models.CreateCardHeaders(
                    x_acs_dingtalk_access_token=token,
                )
                await self._card_sdk.create_card_with_options_async(
                    create_req, create_headers, runtime
                )

                # Step 2: Deliver card to conversation
                if is_group:
                    open_space_id = f"dtv1.card//IM_GROUP.{conversation_id}"
                    deliver_req = dingtalk_card_models.DeliverCardRequest(
                        out_track_id=out_track_id,
                        user_id_type=1,
                        open_space_id=open_space_id,
                        im_group_open_deliver_model=(
                            dingtalk_card_models.DeliverCardRequestImGroupOpenDeliverModel(
                                robot_code=self._robot_code,
                            )
                        ),
                    )
                else:
                    if not sender_staff_id:
                        sender_staff_id = chat_id.split(":")[-1] if ":" in chat_id else chat_id
                    open_space_id = f"dtv1.card//im_robot.{sender_staff_id}"
                    deliver_req = dingtalk_card_models.DeliverCardRequest(
                        out_track_id=out_track_id,
                        user_id_type=1,
                        open_space_id=open_space_id,
                        im_robot_open_deliver_model=(
                            dingtalk_card_models.DeliverCardRequestImRobotOpenDeliverModel(
                                robot_code=self._robot_code,
                            )
                        ),
                    )

                deliver_headers = dingtalk_card_models.DeliverCardHeaders(
                    x_acs_dingtalk_access_token=token,
                )
                await self._card_sdk.deliver_card_with_options_async(
                    deliver_req, deliver_headers, runtime
                )

                _PENDING_APPROVALS[out_track_id] = {
                    "session_key": session_key,
                    "chat_id": chat_id,
                    "sender_staff_id": sender_staff_id,
                    "allowed_approvers": self._allowed_approvers_str,
                    "title": card_param_map["title"],
                }

                logger.info(
                    "[dingtalk-approval] Approval card sent out_track_id=%s session=%s",
                    out_track_id, session_key,
                )
                # Don't expose out_track_id as message_id — approval cards are
                # not streaming cards; the gateway would otherwise try to
                # finalize/edit them via edit_message() and get 400 errors.
                return SendResult(success=True)

            except Exception as e:
                logger.error("[dingtalk-approval] send_exec_approval failed: %s", e, exc_info=True)
                return SendResult(success=False, error=str(e), retryable=True)

        # ── update card after button click ─────────────────────────────────

        async def _update_approval_card(self, out_track_id: str, label: str) -> None:
            """Incremental card update via PUT /v1.0/card/instances + updateCardDataByKey=True.

            Only status and result_label are changed; title/content are preserved
            because we use incremental update (updateCardDataByKey=True) rather than
            full replacement.
            """
            token = await self._get_access_token()
            if not token:
                return
            try:
                import urllib.request
                now = datetime.now().strftime("%H:%M")
                payload = json.dumps({
                    "outTrackId": out_track_id,
                    "cardData": {
                        "cardParamMap": {
                            "status": "resolved",
                            "result_label": f"{label}  {now}",
                        }
                    },
                    "cardUpdateOptions": {"updateCardDataByKey": True},
                }, ensure_ascii=False).encode()
                req = urllib.request.Request(
                    "https://api.dingtalk.com/v1.0/card/instances",
                    data=payload,
                    headers={
                        "Content-Type": "application/json",
                        "x-acs-dingtalk-access-token": token,
                    },
                    method="PUT",
                )
                loop = asyncio.get_event_loop()

                def _do_put():
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        return json.loads(resp.read().decode())

                data = await loop.run_in_executor(None, _do_put)
                logger.info(
                    "[dingtalk-approval] Card updated out_track_id=%s => %s resp=%s",
                    out_track_id, label, data,
                )
            except Exception as e:
                logger.warning("[dingtalk-approval] Card update failed: %s", e)

    return _DingTalkApprovalAdapter


# ── Plugin entry points ────────────────────────────────────────────────────

def check_requirements() -> bool:
    try:
        _, check_dingtalk_requirements = _get_base_dingtalk_adapter()
        return check_dingtalk_requirements()
    except Exception:
        return False


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    client_id = extra.get("client_id") or os.getenv("DINGTALK_CLIENT_ID", "")
    client_secret = extra.get("client_secret") or os.getenv("DINGTALK_CLIENT_SECRET", "")
    return bool(client_id and client_secret)


def register(ctx: Any) -> None:
    """Called by Hermes plugin loader — registers as the 'dingtalk' platform override."""
    try:
        AdapterClass = _build_adapter_class()
    except Exception as e:
        logger.error("[dingtalk-approval] Failed to build adapter class: %s", e)
        return

    # Import cron/standalone delivery helpers from the base DingTalk plugin.
    # The approval plugin overrides 'dingtalk' via last-writer-wins; without
    # re-passing these, cron jobs that deliver=dingtalk will fail with
    # "standalone_sender_fn not registered".
    try:
        DingTalkBase, _ = _get_base_dingtalk_adapter()
        from plugins.platforms.dingtalk.adapter import (  # type: ignore[import-not-found]
            _standalone_send,
            _apply_yaml_config,
            interactive_setup,
            _is_connected,
        )
        _extra_kwargs = dict(
            is_connected=_is_connected,
            validate_config=_is_connected,
            required_env=["DINGTALK_CLIENT_ID", "DINGTALK_CLIENT_SECRET"],
            install_hint="pip install 'dingtalk-stream>=0.20' httpx",
            setup_fn=interactive_setup,
            apply_yaml_config_fn=_apply_yaml_config,
            allowed_users_env="DINGTALK_ALLOWED_USERS",
            allow_all_env="DINGTALK_ALLOW_ALL_USERS",
            cron_deliver_env_var="DINGTALK_HOME_CHANNEL",
            standalone_sender_fn=_standalone_send,
            allow_update_command=True,
        )
    except Exception as e:
        logger.warning("[dingtalk-approval] Could not import cron helpers from base plugin: %s", e)
        _extra_kwargs = {}

    ctx.register_platform(
        name="dingtalk",
        label="DingTalk (approval cards)",
        adapter_factory=lambda cfg: AdapterClass(cfg),
        check_fn=check_requirements,
        emoji="🔔",
        platform_hint=(
            "You are on DingTalk. Use plain text and markdown. "
            "Dangerous commands show interactive approval cards with buttons."
        ),
        **_extra_kwargs,
    )
    logger.info("[dingtalk-approval] Registered 'dingtalk' platform with 4-button approval cards")
