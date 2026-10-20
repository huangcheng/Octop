"""Per-turn delegating backend (spec §6.2).

Harness binds one backend per agent at construction. Dashboard chat freezes a
turn-scoped target via a :class:`~contextvars.ContextVar` so concurrent
threads on the same agent keep independent routing. IM/cron leave the
context unset → hub. Worker threads that hop via ``asyncio.to_thread`` inherit
the context; callers that use raw executors must pass the frozen backend
explicitly.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any, cast

from deepagents.backends.protocol import (
    DeleteResult,
    EditResult,
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
    GlobResult,
    GrepResult,
    LsResult,
    ReadResult,
    SandboxBackendProtocol,
    WriteResult,
)

from octop.infra.errors import ErrorCode, OctopError
from octop.infra.runtime.turn_routing import is_hub_routable_path

#: Frozen backend for the current async turn (``None`` → hub fallback).
_turn_backend: ContextVar[SandboxBackendProtocol | None] = ContextVar(
    "octop_turn_backend", default=None
)


def get_turn_backend() -> SandboxBackendProtocol | None:
    """Return the frozen turn backend, or ``None`` when unset (hub)."""
    return _turn_backend.get()


def set_turn_backend(
    backend: SandboxBackendProtocol | None,
) -> Token[SandboxBackendProtocol | None]:
    """Bind *backend* as the turn-scoped override; return a reset token."""
    return _turn_backend.set(backend)


def reset_turn_backend(token: Token[SandboxBackendProtocol | None]) -> None:
    _turn_backend.reset(token)


@contextmanager
def turn_backend_scope(backend: SandboxBackendProtocol | None) -> Iterator[None]:
    """Context manager that freezes *backend* for the duration of a turn."""
    token = set_turn_backend(backend)
    try:
        yield
    finally:
        reset_turn_backend(token)


class FailClosedBackend(SandboxBackendProtocol):
    """Every workspace file/exec op fails with a fixed :class:`OctopError`.

    Used when ``run_target=invalid`` — fail-closed **before** any prefix-split
    so revoked machines never silently fall back to hub (§6.2).
    """

    def __init__(self, error: OctopError) -> None:
        self._error = error

    @property
    def id(self) -> str:  # noqa: A003 - SandboxBackendProtocol
        return f"fail_closed:{self._error.code.value}"

    def _raise(self) -> None:
        raise self._error

    def _err_msg(self) -> str:
        return self._error.message

    def ls(self, path: str) -> LsResult:
        return LsResult(error=self._err_msg())

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return ReadResult(error=self._err_msg())

    def write(self, file_path: str, content: str) -> WriteResult:
        return WriteResult(error=self._err_msg())

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,  # noqa: FBT001, FBT002
    ) -> EditResult:
        return EditResult(error=self._err_msg())

    def delete(self, file_path: str) -> DeleteResult:
        return DeleteResult(error=self._err_msg())

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return GlobResult(error=self._err_msg())

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        return GrepResult(error=self._err_msg())

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        msg = self._err_msg()
        return [FileUploadResponse(path=p, error=msg) for p, _ in files]

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        msg = self._err_msg()
        return [FileDownloadResponse(path=p, content=None, error=msg) for p in paths]

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        return ExecuteResponse(output=f"Error: {self._err_msg()}", exit_code=None, truncated=False)


class PrefixSplitRemoteBackend(SandboxBackendProtocol):
    """Route content/exec to *remote*; hub-routable prefixes to *hub* (§6.1).

    ``execute`` always goes to *remote* (machine path-class). Offline remote
    ops surface ``RUNTIME_OFFLINE`` from the remote proxy; hub prefixes stay
    usable without a live session.
    """

    def __init__(self, remote: SandboxBackendProtocol, hub: Any) -> None:
        self._remote = remote
        self._hub = hub

    @property
    def id(self) -> str:  # noqa: A003
        remote_id = getattr(self._remote, "id", None) or type(self._remote).__name__
        return f"prefix_split:{remote_id}"

    @property
    def remote(self) -> SandboxBackendProtocol:
        return self._remote

    @property
    def hub(self) -> Any:
        return self._hub

    def _files(self, path: str) -> Any:
        return self._hub if is_hub_routable_path(path) else self._remote

    def ls(self, path: str) -> LsResult:
        return cast(LsResult, self._files(path or "/").ls(path))

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return cast(ReadResult, self._files(file_path).read(file_path, offset, limit))

    def write(self, file_path: str, content: str) -> WriteResult:
        return cast(WriteResult, self._files(file_path).write(file_path, content))

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,  # noqa: FBT001, FBT002
    ) -> EditResult:
        return cast(
            EditResult,
            self._files(file_path).edit(file_path, old_string, new_string, replace_all),
        )

    def delete(self, file_path: str) -> DeleteResult:
        deleter = getattr(self._files(file_path), "delete", None)
        if deleter is None:
            return DeleteResult(error="delete is not supported")
        return cast(DeleteResult, deleter(file_path))

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        # Prefer hub when the search root is hub-routable; otherwise remote.
        root = path or "/"
        return cast(GlobResult, self._files(root).glob(pattern, path))

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        root = path or "/"
        return cast(
            GrepResult,
            self._files(root).grep(pattern, path, glob, max_count=max_count),
        )

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        out: list[FileUploadResponse] = []
        for path, content in files:
            uploader = getattr(self._files(path), "upload_files", None)
            if uploader is None:
                out.append(FileUploadResponse(path=path, error="upload unsupported"))
                continue
            out.extend(cast(list[FileUploadResponse], uploader([(path, content)])))
        return out

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        out: list[FileDownloadResponse] = []
        for path in paths:
            downloader = getattr(self._files(path), "download_files", None)
            if downloader is None:
                out.append(
                    FileDownloadResponse(path=path, content=None, error="download unsupported")
                )
                continue
            out.extend(cast(list[FileDownloadResponse], downloader([path])))
        return out

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        return self._remote.execute(command, timeout=timeout)


class TurnDelegatingBackend(SandboxBackendProtocol):
    """Stable agent backend that defers to the turn-scoped ContextVar.

    Constructed once at agent start around the hub backend. When the turn
    context is unset (IM/cron/gateway, or dashboard hub target), ops hit
    ``hub``. When a dashboard turn freezes a remote/fail-closed backend into
    the ContextVar, ops hit that target instead.
    """

    def __init__(self, hub: Any) -> None:
        self._hub = hub

    @property
    def id(self) -> str:  # noqa: A003
        hub_id = getattr(self._hub, "id", None) or type(self._hub).__name__
        return f"turn_delegate:{hub_id}"

    @property
    def hub(self) -> Any:
        return self._hub

    def _active(self) -> Any:
        return get_turn_backend() or self._hub

    def __getattr__(self, name: str) -> Any:
        # Forward mount helpers (mkdir_path, _resolve_path, virtual_mode, …)
        # so BackendWorkspace treats this wrapper like the hub leg.
        return getattr(self._active(), name)

    # -- file ops -----------------------------------------------------------

    def ls(self, path: str) -> LsResult:
        return cast(LsResult, self._active().ls(path))

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        return cast(ReadResult, self._active().read(file_path, offset, limit))

    def write(self, file_path: str, content: str) -> WriteResult:
        return cast(WriteResult, self._active().write(file_path, content))

    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,  # noqa: FBT001, FBT002
    ) -> EditResult:
        return cast(
            EditResult,
            self._active().edit(file_path, old_string, new_string, replace_all),
        )

    def delete(self, file_path: str) -> DeleteResult:
        deleter = getattr(self._active(), "delete", None)
        if deleter is None:
            return DeleteResult(error="delete is not supported")
        return cast(DeleteResult, deleter(file_path))

    def glob(self, pattern: str, path: str | None = None) -> GlobResult:
        return cast(GlobResult, self._active().glob(pattern, path))

    def grep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        return cast(
            GrepResult,
            self._active().grep(pattern, path, glob, max_count=max_count),
        )

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        uploader = getattr(self._active(), "upload_files", None)
        if uploader is None:
            return [FileUploadResponse(path=p, error="upload unsupported") for p, _ in files]
        return cast(list[FileUploadResponse], uploader(files))

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        downloader = getattr(self._active(), "download_files", None)
        if downloader is None:
            return [
                FileDownloadResponse(path=p, content=None, error="download unsupported")
                for p in paths
            ]
        return cast(list[FileDownloadResponse], downloader(paths))

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        active = self._active()
        execute = getattr(active, "execute", None)
        if execute is None:
            return ExecuteResponse(
                output="Error: execution is not supported by the active backend",
                exit_code=None,
                truncated=False,
            )
        return cast(ExecuteResponse, execute(command, timeout=timeout))


def build_fail_closed_revoked() -> FailClosedBackend:
    """Fail-closed backend for ``run_target=invalid``."""
    return FailClosedBackend(
        OctopError(
            ErrorCode.RUNTIME_REVOKED,
            "runtime target is invalid; re-select hub or another machine",
        )
    )


def build_machine_turn_backend(
    *,
    remote: SandboxBackendProtocol,
    hub: Any,
) -> PrefixSplitRemoteBackend:
    """Prefix-split composite for an online/offline machine ``run_target``."""
    return PrefixSplitRemoteBackend(remote, hub)


__all__ = [
    "FailClosedBackend",
    "PrefixSplitRemoteBackend",
    "TurnDelegatingBackend",
    "build_fail_closed_revoked",
    "build_machine_turn_backend",
    "get_turn_backend",
    "reset_turn_backend",
    "set_turn_backend",
    "turn_backend_scope",
]
