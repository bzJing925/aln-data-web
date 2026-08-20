"""merge_share_pack 测试：迷你 canonical + 手工构造分享包 → 合并契约。

运行（本目录 ci/ 下）：pytest test_merge_share_pack.py -q
依赖：pandas、pyarrow（CI runner 同环境）。
"""

import gzip
import io
import json
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from merge_share_pack import (  # noqa: E402
    PACK_COLUMNS,
    PackError,
    merge_pack,
    validate_pack,
)


def _write_arrow_gz(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    with gzip.open(str(path), "wb", compresslevel=6) as sink:
        with pa.ipc.new_file(sink, table.schema) as w:
            w.write_table(table)


def _make_repo(root: Path) -> Path:
    """构造迷你 canonical store。"""
    (root / "data/meta").mkdir(parents=True)
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "batch_id": [1, 1],
            "batch_no": ["#2", "#2"],
            "original_filename": ["a.s1p", "b.s1p"],
            "display_name": ["a", "b"],
            "mark": ["A1", "A1"],
            "wafer": [1.0, 1.0],
            "folder_name": ["S11", "S22"],
            "coord": ["X-1Y-1", "X-1Y-2"],
            "x": [-1.0, -1.0],
            "y": [-1.0, -2.0],
            "eg": [1.0, 1.0],
            "fl": [0.5, 0.5],
            "ag": [None, None],
            "pf": ["N", "N"],
            "area_n": [1.0, 1.0],
            "area_um2": [700.0, 700.0],
            "fs_ghz": [4.8, 4.81],
            "fp_ghz": [5.1, 5.11],
            "zs_ohm": [3.0, 3.1],
            "zp_ohm": [900.0, 880.0],
            "qs": [500.0, 490.0],
            "qp": [600.0, 610.0],
            "qs_bodeq": [480.0, 470.0],
            "qp_bodeq": [590.0, 600.0],
            "dbqs": [1.0, 1.0],
            "dbqp": [1.1, 1.1],
            "bodeq_fitted": [480.0, 470.0],
            "bodeq_smooth": [481.0, 471.0],
            "bodeq_raw": [479.0, 469.0],
            "fbode_ghz": [4.82, 4.83],
            "k2eff_pct": [7.0, 7.0],
            "fp2_ghz": [None, None],
            "fs2_ghz": [None, None],
            "zp2_ohm": [None, None],
            "zs2_ohm": [None, None],
            "deembedded": [False, False],
            "s_param_port": ["S11", "S22"],
        }
    )
    _write_arrow_gz(df, root / "data/devices.arrow.gz")
    batches = [
        {
            "id": 1, "batch_no": "#2", "mapping_id": 1, "f_start_ghz": None,
            "f_end_ghz": None, "deembedded": False, "deembed_method": None,
            "process_type": "S2P", "uploaded_at": "2026-01-01", "uploaded_by": "",
            "device_count": 2, "task_id": None,
        }
    ]
    mappings = [
        {
            "id": 1, "name": "ELB003", "entry_count": 1, "uploaded_at": "",
            "entries": [
                {"id": 1, "mapping_id": 1, "mark": "A1", "description": "EG1 FL0.5 700&5500",
                 "eg": 1.0, "fl": 0.5, "ag": None, "area_s11": 700, "area_s22": 5500,
                 "has_pf": False}
            ],
        }
    ]
    (root / "data/meta/batches.json").write_text(json.dumps(batches), encoding="utf-8")
    (root / "data/meta/mappings.json").write_text(json.dumps(mappings), encoding="utf-8")
    (root / "data/meta/process.json").write_text("{}", encoding="utf-8")
    (root / "data/meta/fields.json").write_text("{}", encoding="utf-8")
    (root / "data/meta/stats.json").write_text(
        json.dumps({"batches": 1, "devices": 2, "mappings": 1}), encoding="utf-8"
    )
    (root / "surr").mkdir()
    (root / "surr/surr_meta.json").write_text("{}", encoding="utf-8")
    return root


def _make_pack(path: Path, batch_no: str = "#9", n: int = 2, sha_bad: bool = False,
               mapping_name: str = "ELB003") -> Path:
    rows = []
    for i in range(n):
        r = {c: "" for c in PACK_COLUMNS}
        r.update(
            {
                "original_filename": f"f{i}.s1p", "display_name": f"f{i}", "mark": "A1",
                "wafer": 2, "folder_name": "S11", "coord": f"X0Y{i}", "x": 0, "y": i,
                "eg": 1.0, "fl": 0.5, "pf": "N", "area_n": 1, "area_um2": 700,
                "fs_ghz": 4.8 + i * 0.01, "fp_ghz": 5.1 + i * 0.01, "zs_ohm": 3.0,
                "zp_ohm": 900.0, "qs": 500.0, "qp": 600.0, "qs_bodeq": 480.0,
                "qp_bodeq": 590.0, "dbqs": 1.0, "dbqp": 1.1, "bodeq_fitted": 480.0,
                "bodeq_smooth": 481.0, "bodeq_raw": 479.0, "fbode_ghz": 4.82,
                "k2eff_pct": 7.0, "deembedded": False, "s_param_port": "S11",
            }
        )
        rows.append(r)
    buf = io.StringIO()
    import csv as csvmod

    w = csvmod.DictWriter(buf, fieldnames=PACK_COLUMNS)
    w.writeheader()
    w.writerows(rows)
    blob = gzip.compress(buf.getvalue().encode(), compresslevel=6)
    import hashlib

    meta = {
        "format_version": 1,
        "batch_no": batch_no,
        "mapping_name": mapping_name,
        "f_start_ghz": None,
        "f_end_ghz": None,
        "deembedded": False,
        "process_type": "S2P",
        "device_count": n,
        "failure_count": 0,
        "created_at": "2026-08-20T00:00:00+00:00",
        "generator": "test",
        "sha256_devices_csv_gz": "0" * 64 if sha_bad else hashlib.sha256(blob).hexdigest(),
    }
    mapping = {
        "name": mapping_name,
        "entries": [
            {"mark": "A1", "description": "EG1 FL0.5 700&5500", "eg": 1.0, "fl": 0.5,
             "ag": None, "area_s11": 700, "area_s22": 5500, "has_pf": False}
        ],
    }
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("meta.json", json.dumps(meta))
        zf.writestr("mapping.json", json.dumps(mapping))
        zf.writestr("devices.csv.gz", blob)
    return path


def test_merge_happy(tmp_path):
    repo = _make_repo(tmp_path / "repo")
    pack = _make_pack(tmp_path / "pack.zip", n=2)
    report = merge_pack(pack, repo)
    assert report["batch_no"] == "#9"
    assert report["total_devices"] == 4
    assert "mapping_added" not in report  # ELB003 已存在

    with gzip.open(repo / "data/devices.arrow.gz", "rb") as f:
        t = pa.ipc.open_file(pa.py_buffer(f.read())).read_all()
    assert t.num_rows == 4
    assert set(t.column("batch_no").to_pylist()) == {"#2", "#9"}
    ids = t.column("id").to_pylist()
    assert ids == [1, 2, 3, 4]

    batches = json.loads((repo / "data/meta/batches.json").read_text())
    assert batches[-1]["batch_no"] == "#9" and batches[-1]["id"] == 2
    stats = json.loads((repo / "data/meta/stats.json").read_text())
    assert stats["devices"] == 4 and stats["batches"] == 2
    manifest = json.loads((repo / "webdata/manifest.json").read_text())
    assert "devices.arrow.gz" in manifest["files"]


def test_new_mapping_appended(tmp_path):
    repo = _make_repo(tmp_path / "repo")
    pack = _make_pack(tmp_path / "pack.zip", batch_no="#10", mapping_name="NEWMAP")
    report = merge_pack(pack, repo)
    assert report["mapping_added"] == "NEWMAP"
    mappings = json.loads((repo / "data/meta/mappings.json").read_text())
    assert len(mappings) == 2 and mappings[-1]["entries"][0]["mark"] == "A1"


def test_duplicate_batch_rejected(tmp_path):
    repo = _make_repo(tmp_path / "repo")
    pack = _make_pack(tmp_path / "pack.zip", batch_no="#2")
    with pytest.raises(PackError, match="已存在"):
        validate_pack(pack, repo)


def test_bad_sha_rejected(tmp_path):
    repo = _make_repo(tmp_path / "repo")
    pack = _make_pack(tmp_path / "pack.zip", sha_bad=True)
    with pytest.raises(PackError, match="校验和"):
        validate_pack(pack, repo)


def test_check_only_writes_nothing(tmp_path):
    repo = _make_repo(tmp_path / "repo")
    pack = _make_pack(tmp_path / "pack.zip")
    before = (repo / "data/devices.arrow.gz").read_bytes()
    report = validate_pack(pack, repo)
    assert report["devices"] == 2
    assert (repo / "data/devices.arrow.gz").read_bytes() == before
    assert not (repo / "webdata").exists()
