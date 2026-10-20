"""Enrollment lifecycle side-effects (revoke / rotate / allow_exec).

Keeps HTTP routers thin: close live WS, invalidate cached proxies, mark
threads ``invalid``, clear agent defaults, and push dashboard notify frames.
"""

from __future__ import annotations

import logging
from typing import Any

from octop.infra.runtime.backend import RuntimeBackendHub
from octop.infra.runtime.registry import RuntimeSessionRegistry

logger = logging.getLogger("octop.runtime.lifecycle")

#: Protocol close code for revoke (maps to ``RUNTIME_REVOKED`` on in-flight ops).
CLOSE_REVOKED = 4002


async def apply_revoke_side_effects(
    *,
    enrollment_id: str,
    owner_user_id: int,
    registry: RuntimeSessionRegistry | None,
    runtime_hub: RuntimeBackendHub | None,
    thread_repo: Any | None,
    agent_repo: Any | None,
    ws_hub: Any | None = None,
) -> dict[str, int]:
    """Run post-revoke deltas from spec §8.2 / §6.2.

    Returns counts for tests/audit: ``{"closed": n, "threads": n, "agents": n,
    "proxies": n}``.
    """
    eid = str(enrollment_id).strip()
    closed = 0
    if registry is not None:
        closed = await registry.close_enrollment(
            eid, code=CLOSE_REVOKED, reason="enrollment revoked"
        )
    proxies = 0
    if runtime_hub is not None:
        proxies = runtime_hub.invalidate(eid)
    threads = 0
    if thread_repo is not None and hasattr(thread_repo, "invalidate_run_targets"):
        threads = int(thread_repo.invalidate_run_targets(eid) or 0)
    agents = 0
    if agent_repo is not None and hasattr(agent_repo, "clear_default_run_target"):
        agents = int(agent_repo.clear_default_run_target(eid) or 0)
    if ws_hub is not None and hasattr(ws_hub, "push_to_user"):
        try:
            await ws_hub.push_to_user(
                int(owner_user_id),
                {"type": "runtime.revoked", "enrollment_id": eid},
            )
        except Exception:  # noqa: BLE001 - notify must not undo revoke
            logger.debug("runtime.revoked push failed", exc_info=True)
    return {
        "closed": closed,
        "threads": threads,
        "agents": agents,
        "proxies": proxies,
    }


async def apply_session_drop_side_effects(
    *,
    enrollment_id: str,
    owner_user_id: int | None,
    runtime_hub: RuntimeBackendHub | None,
    registry: RuntimeSessionRegistry | None,
    ws_hub: Any | None = None,
    online: bool | None = None,
) -> None:
    """Invalidate proxies + push ``runtime.status`` after a session goes down."""
    eid = str(enrollment_id).strip()
    if runtime_hub is not None:
        runtime_hub.invalidate(eid)
    if ws_hub is None or owner_user_id is None or not hasattr(ws_hub, "push_to_user"):
        return
    is_online = online
    if is_online is None and registry is not None:
        is_online = registry.online(eid)
    try:
        await ws_hub.push_to_user(
            int(owner_user_id),
            {
                "type": "runtime.status",
                "enrollment_id": eid,
                "online": bool(is_online),
            },
        )
    except Exception:  # noqa: BLE001
        logger.debug("runtime.status push failed", exc_info=True)
