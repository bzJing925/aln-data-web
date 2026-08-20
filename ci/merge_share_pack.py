"""合并同事分享包 → canonical store + webdata（GitHub Actions 用，仅依赖 pandas+pyarrow）。

分享包格式（format_version=1，由主仓库 backend/scripts/make_share_pack.py 生成）：
    meta.json        {format_version, batch_no, mapping_name, device_count,
                      process_type, f_start_ghz, f_end_ghz, deembedded,
                      created_at, sha256_devices_csv_gz, ...}
    mapping.json     {name, entries: [{mark, description, eg, fl, ag,
                      area_s11, area_s22, has_pf}]}
    devices.csv.gz   每行一个器件端口的提取参数（列见 PACK_COLUMNS）

用法：
    python ci/merge_share_pack.py uploads/<pack>.zip --repo-root .
    python ci/merge_share_pack.py uploads/<pack>.zip --check-only   # PR 校验，不落盘

合并产物：
    data/devices.arrow.gz       canonical 器件表（追加新批次行）
    data/meta/batches.json      追加批次条目
    data/meta/mappings.json     新对照表时追加
    data/meta/stats.json        重算
    webdata/                    可直接拷到 gh-pages 的快照（含 manifest.json + surr/ 拷贝）
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import shutil
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa

FORMAT_VERSION = 1

# 与 make_share_pack.PACK_COLUMNS 保持一致（契约）
PACK_COLUMNS = [
    "original_filename", "display_name", "mark", "wafer", "folder_name", "coord",
    "x", "y", "eg", "fl", "ag", "pf", "area_n", "area_um2",
    "fs_ghz", "fp_ghz", "zs_ohm", "zp_ohm", "qs", "qp", "qs_bodeq", "qp_bodeq",
    "dbqs", "dbqp", "bodeq_fitted", "bodeq_smooth", "bodeq_raw", "fbode_ghz",
    "k2eff_pct", "fp2_ghz", "fs2_ghz", "zp2_ohm", "zs2_ohm", "deembedded", "s_param_port",
]

REQUIRED_NUMERIC = ["fs_ghz", "fp_ghz"]


class PackError(Exception):
    """分享包校验失败。"""


def _read_arrow_gz(path: Path) -> pa.Table:
    with gzip.open(path, "rb") as f:
        return pa.ipc.open_file(pa.py_buffer(f.read())).read_all()


def _write_arrow_gz(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    with gzip.open(str(path), "wb", compresslevel=6) as sink:
        with pa.ipc.new_file(sink, table.schema) as w:
            w.write_table(table)


def load_pack(pack_path: Path) -> tuple[dict, dict, pd.DataFrame]:
    """读取并校验分享包，返回 (meta, mapping, devices_df)。校验失败抛 PackError。"""
    if not pack_path.exists():
        raise PackError(f"文件不存在: {pack_path}")
    try:
        zf = zipfile.ZipFile(pack_path)
    except zipfile.BadZipFile as e:
        raise PackError(f"不是合法 zip: {e}") from e
    names = set(zf.namelist())
    for need in ("meta.json", "mapping.json", "devices.csv.gz"):
        if need not in names:
            raise PackError(f"分享包缺少 {need}")

    meta = json.loads(zf.read("meta.json"))
    mapping = json.loads(zf.read("mapping.json"))
    blob = zf.read("devices.csv.gz")

    if meta.get("format_version") != FORMAT_VERSION:
        raise PackError(f"format_version={meta.get('format_version')} 不受支持（当前 {FORMAT_VERSION}）")
    sha = hashlib.sha256(blob).hexdigest()
    if sha != meta.get("sha256_devices_csv_gz"):
        raise PackError("devices.csv.gz 校验和不匹配（文件损坏或串包）")

    df = pd.read_csv(io.BytesIO(gzip.decompress(blob)), na_values=["", "None", "null"])
    missing = [c for c in PACK_COLUMNS if c not in df.columns]
    if missing:
        raise PackError(f"devices.csv 缺列: {missing}")
    if len(df) != meta.get("device_count"):
        raise PackError(f"行数 {len(df)} 与 meta.device_count={meta.get('device_count')} 不一致")

    # 类型归一
    df["deembedded"] = df["deembedded"].map(
        lambda v: v if isinstance(v, bool) else str(v).strip().lower() in ("true", "1", "y")
    )
    for c in REQUIRED_NUMERIC:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        if df[c].isna().all():
            raise PackError(f"列 {c} 全部为空/非数值")
    # 物理粗检：fs < fp、频率量级
    bad = df[(df["fs_ghz"] >= df["fp_ghz"]) | (df["fs_ghz"] < 0.1) | (df["fs_ghz"] > 20)]
    if len(bad) > len(df) * 0.5:
        raise PackError(f"超过一半行 fs/fp 异常（{len(bad)}/{len(df)}），疑似提取错误")

    return meta, mapping, df


def validate_pack(pack_path: Path, repo_root: Path) -> dict:
    """只校验（PR 场景），返回报告 dict。"""
    meta, mapping, df = load_pack(pack_path)
    batches = json.loads((repo_root / "data/meta/batches.json").read_text())
    existing = {b["batch_no"] for b in batches}
    if meta["batch_no"] in existing:
        raise PackError(f"批次号 {meta['batch_no']} 已存在（如需重传请先联系管理员删除）")
    mappings = json.loads((repo_root / "data/meta/mappings.json").read_text())
    mapping_state = "existing" if any(m["name"] == meta["mapping_name"] for m in mappings) else "new"
    return {
        "batch_no": meta["batch_no"],
        "devices": int(len(df)),
        "mapping": f"{meta['mapping_name']}（{mapping_state}）",
        "fs_range": f"{df['fs_ghz'].min():.3f}–{df['fs_ghz'].max():.3f} GHz",
        "failures_in_pack": meta.get("failure_count", 0),
        "created_at": meta.get("created_at"),
    }


def merge_pack(pack_path: Path, repo_root: Path) -> dict:
    """校验 + 合并 + 重生成 webdata。返回报告 dict。"""
    report = validate_pack(pack_path, repo_root)
    meta, mapping, df_new = load_pack(pack_path)

    meta_dir = repo_root / "data/meta"
    batches = json.loads((meta_dir / "batches.json").read_text())
    mappings = json.loads((meta_dir / "mappings.json").read_text())

    # mapping：不存在则追加（新 id）
    m = next((x for x in mappings if x["name"] == meta["mapping_name"]), None)
    if m is None:
        new_mid = max((x["id"] for x in mappings), default=0) + 1
        base_eid = max(
            (e["id"] for x in mappings for e in x.get("entries", [])), default=0
        )
        entries = []
        for i, e in enumerate(mapping["entries"], 1):
            entries.append({"id": base_eid + i, "mapping_id": new_mid, **e})
        m = {
            "id": new_mid,
            "name": meta["mapping_name"],
            "entry_count": len(entries),
            "uploaded_at": meta["created_at"],
            "entries": entries,
        }
        mappings.append(m)
        report["mapping_added"] = meta["mapping_name"]
    mapping_id = m["id"]

    # batch 条目
    new_bid = max((b["id"] for b in batches), default=0) + 1
    batches.append(
        {
            "id": new_bid,
            "batch_no": meta["batch_no"],
            "mapping_id": mapping_id,
            "f_start_ghz": meta.get("f_start_ghz"),
            "f_end_ghz": meta.get("f_end_ghz"),
            "deembedded": bool(meta.get("deembedded")),
            "deembed_method": None,
            "process_type": meta.get("process_type"),
            "uploaded_at": meta.get("created_at"),
            "uploaded_by": "share-pack",
            "device_count": meta["device_count"],
            "task_id": None,
        }
    )

    # devices：续 id、补 batch_id/batch_no，按 canonical schema 对齐后拼接
    df_can = _read_arrow_gz(repo_root / "data/devices.arrow.gz").to_pandas()
    id0 = int(df_can["id"].max()) + 1 if len(df_can) else 1
    df_new = df_new.copy()
    df_new["id"] = range(id0, id0 + len(df_new))
    df_new["batch_id"] = new_bid
    df_new["batch_no"] = meta["batch_no"]
    df_new = df_new[["id", "batch_id", "batch_no", *PACK_COLUMNS]]
    df_all = pd.concat([df_can, df_new], ignore_index=True)
    _write_arrow_gz(df_all, repo_root / "data/devices.arrow.gz")

    # stats 重算
    stats = {
        "batches": len(batches),
        "devices": int(len(df_all)),
        "mappings": len(mappings),
        "disk_used_gb": None,
        "disk_free_gb": None,
        "tasks_pending": 0,
        "tasks_running": 0,
    }

    (meta_dir / "batches.json").write_text(json.dumps(batches, ensure_ascii=False), encoding="utf-8")
    (meta_dir / "mappings.json").write_text(
        json.dumps(mappings, ensure_ascii=False), encoding="utf-8"
    )
    (meta_dir / "stats.json").write_text(json.dumps(stats), encoding="utf-8")

    # 重生成 webdata/（拷到 gh-pages 即生效；surr/ 原样拷贝）
    webdata = repo_root / "webdata"
    if webdata.exists():
        shutil.rmtree(webdata)
    webdata.mkdir()
    shutil.copy(repo_root / "data/devices.arrow.gz", webdata / "devices.arrow.gz")
    for name in ("batches", "mappings", "stats"):
        shutil.copy(meta_dir / f"{name}.json", webdata / f"{name}.json")
    for name in ("process", "fields"):
        shutil.copy(meta_dir / f"{name}.json", webdata / f"{name}.json")
    surr_files = []
    surr_dir = repo_root / "surr"
    if surr_dir.exists():
        for f in surr_dir.iterdir():
            shutil.copy(f, webdata / f.name)
            surr_files.append(f.name)
    files = ["devices.arrow.gz", "batches.json", "mappings.json", "process.json",
             "stats.json", "fields.json", *sorted(surr_files)]
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "files": files,
        "missing": [],
        "counts": {"devices": int(len(df_all)), "batches": len(batches)},
    }
    (webdata / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    report["total_devices"] = int(len(df_all))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("pack", help="分享包 zip 路径")
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--check-only", action="store_true")
    args = ap.parse_args()
    try:
        if args.check_only:
            report = validate_pack(Path(args.pack), Path(args.repo_root))
            print("校验通过:", json.dumps(report, ensure_ascii=False))
        else:
            report = merge_pack(Path(args.pack), Path(args.repo_root))
            print("合并完成:", json.dumps(report, ensure_ascii=False))
    except PackError as e:
        print(f"校验失败: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
