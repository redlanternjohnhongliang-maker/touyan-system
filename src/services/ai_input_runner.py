from __future__ import annotations

import subprocess
import sys
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
        p = Path(line)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        if p.suffix.lower() == ".json" and p.exists():
            json_paths.append(p)
        elif p.suffix.lower() == ".md" and p.exists():
            md_paths.append(p)
    return json_paths, md_paths


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
) -> AiInputExportResult:
    # 如果传了 symbols（多股票逗号分隔），优先使用
    if symbols.strip():
        resolved_symbols = symbols.strip()
    else:
        resolved_symbols = ""
        if symbol.strip():
            resolved_symbols = resolve_stock_code(symbol.strip()) or "000000"

    script_path = PROJECT_ROOT / "tools" / "export_merged_raw_content.py"
    cmd = [
        sys.executable,
        str(script_path),
    ]
    # 多股票用 --symbols，单股票用 --symbol
    if "," in resolved_symbols:
        cmd.extend(["--symbols", resolved_symbols])
    else:
        cmd.extend(["--symbol", resolved_symbols or "000000"])
    cmd.extend([
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
    ])
    if target_date:
        cmd.extend(["--target-date", target_date])
    if allow_fallback:
        cmd.append("--allow-fallback")
    if disable_weekend_shift:
        cmd.append("--disable-weekend-shift")
    if overwrite_latest:
        cmd.append("--overwrite-latest")

    import os as _os
    env = _os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(60, timeout_sec),
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "AI输入文件导出失败\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    json_paths, md_paths = _collect_output_paths(proc.stdout)
    if not json_paths or not md_paths:
        raise RuntimeError(
            "导出脚本已执行但未识别到输出路径\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    # 兼容旧调用：主文件取第一对
    json_path = json_paths[0]
    md_lookup = {p.stem: p for p in md_paths}
    md_path = md_lookup.get(json_path.stem, md_paths[0])

    payload = {}
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
        stdout=proc.stdout,
        stderr=proc.stderr,
    )
