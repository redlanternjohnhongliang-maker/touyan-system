from __future__ import annotations

import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.services.symbol_resolver import resolve_stock_code


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class AiInputExportResult:
    json_path: Path
    md_path: Path
    json_paths: list[Path]
    md_paths: list[Path]
    payload: dict[str, Any]
    markdown: str
    stdout: str
    stderr: str


def _collect_output_paths(stdout: str | None) -> tuple[list[Path], list[Path]]:
    json_paths: list[Path] = []
    md_paths: list[Path] = []
    if not stdout:
        return json_paths, md_paths
    for line in (ln.strip() for ln in stdout.splitlines() if ln.strip()):
        path = Path(line)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        if path.suffix.lower() == ".json" and path.exists():
            json_paths.append(path)
        elif path.suffix.lower() == ".md" and path.exists():
            md_paths.append(path)
    return json_paths, md_paths


def _build_export_command(
    resolved_symbols: str,
    mode: str,
    target_user: str,
    target_date: str,
    window_days: int,
    allow_fallback: bool,
    disable_weekend_shift: bool,
    out_prefix: str,
    out_dir: str,
    overwrite_latest: bool,
    scope: str,
) -> list[str]:
    script_path = PROJECT_ROOT / "tools" / "export_merged_raw_content.py"
    cmd = [sys.executable, str(script_path)]
    if "," in resolved_symbols:
        cmd.extend(["--symbols", resolved_symbols])
    else:
        cmd.extend(["--symbol", resolved_symbols or "000000"])
    cmd.extend(
        [
            "--mode",
            mode.strip().lower(),
            "--target-user",
            target_user,
            "--window-days",
            str(max(1, int(window_days))),
            "--out-prefix",
            out_prefix,
            "--out-dir",
            out_dir,
            "--scope",
            scope,
        ]
    )
    if target_date:
        cmd.extend(["--target-date", target_date])
    if allow_fallback:
        cmd.append("--allow-fallback")
    if disable_weekend_shift:
        cmd.append("--disable-weekend-shift")
    if overwrite_latest:
        cmd.append("--overwrite-latest")
    return cmd


def _push_event(event_queue: queue.Queue[str] | None, message: str) -> None:
    if event_queue is None:
        return
    text = str(message or "").strip()
    if not text:
        return
    try:
        event_queue.put_nowait(text)
    except Exception:
        pass


def _read_process_pipe(pipe: Any, sink: list[str], event_queue: queue.Queue[str] | None) -> None:
    try:
        for raw_line in iter(pipe.readline, ""):
            line = str(raw_line).rstrip("\r\n")
            sink.append(line)
            _push_event(event_queue, line)
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def run_ai_input_export(
    symbol: str = "",
    symbols: str = "",
    mode: str = "deep",
    target_user: str = "盘前纪要",
    target_date: str = "",
    window_days: int = 1,
    allow_fallback: bool = False,
    disable_weekend_shift: bool = False,
    out_prefix: str = "ai_input_bundle",
    out_dir: str = "tools",
    overwrite_latest: bool = False,
    scope: str = "all",
    timeout_sec: int = 420,
    event_queue: queue.Queue[str] | None = None,
) -> AiInputExportResult:
    if symbols.strip():
        resolved_symbols = symbols.strip()
    else:
        resolved_symbols = ""
        if symbol.strip():
            resolved_symbols = resolve_stock_code(symbol.strip()) or "000000"

    cmd = _build_export_command(
        resolved_symbols=resolved_symbols,
        mode=mode,
        target_user=target_user,
        target_date=target_date,
        window_days=window_days,
        allow_fallback=allow_fallback,
        disable_weekend_shift=disable_weekend_shift,
        out_prefix=out_prefix,
        out_dir=out_dir,
        overwrite_latest=overwrite_latest,
        scope=scope,
    )

    import os as _os

    env = _os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    _push_event(event_queue, "启动导出任务...")

    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    threads = [
        threading.Thread(
            target=_read_process_pipe,
            args=(proc.stdout, stdout_lines, event_queue),
            daemon=True,
        ),
        threading.Thread(
            target=_read_process_pipe,
            args=(proc.stderr, stderr_lines, event_queue),
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()

    try:
        return_code = proc.wait(timeout=max(60, timeout_sec))
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        _push_event(event_queue, f"export timeout (> {max(60, timeout_sec)}s)")
        raise RuntimeError(
            "AI input export timed out\n"
            f"cmd: {' '.join(cmd)}"
        ) from exc
    finally:
        for thread in threads:
            thread.join(timeout=1.0)

    stdout = "\n".join(stdout_lines).strip()
    stderr = "\n".join(stderr_lines).strip()
    if return_code != 0:
        raise RuntimeError(
            "AI input export failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    json_paths, md_paths = _collect_output_paths(stdout)
    if not json_paths or not md_paths:
        raise RuntimeError(
            "Export finished but output paths were not detected\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )

    json_path = json_paths[0]
    md_lookup = {path.stem: path for path in md_paths}
    md_path = md_lookup.get(json_path.stem, md_paths[0])

    payload: dict[str, Any] = {}
    markdown = ""
    try:
        import json

        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    try:
        markdown = md_path.read_text(encoding="utf-8")
    except Exception:
        markdown = ""

    return AiInputExportResult(
        json_path=json_path,
        md_path=md_path,
        json_paths=json_paths,
        md_paths=md_paths,
        payload=payload,
        markdown=markdown,
        stdout=stdout,
        stderr=stderr,
    )
