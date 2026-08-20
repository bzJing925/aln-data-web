# aln-data-web

ALN 谐振器数据平台的静态网站数据仓库（GitHub Pages 部署源）。

- 网站：<https://bzjing925.github.io/aln-data-web/>
- `main` 分支：canonical 数据（`data/`）、代理模型产物（`surr/`）、分享包合并 CI（`ci/`、`.github/workflows/`）
- `gh-pages` 分支：部署的静态站点（不要手工改）

## 同事上传新批次（无需在服务器上挂后端）

平台主机的原始 snp zip 太大（GB 级）传不了 GitHub，所以流程是**本地提取、只传参数**：

1. 在自己电脑上拿到平台代码（内部仓库），进入 `backend/`：
   ```bash
   uv sync
   uv run python scripts/make_share_pack.py /path/数据.zip \
       --batch-no '#5' --mapping /path/对照表.xlsx -o 分享包_5.zip
   ```
   - `--batch-no` 必须与网站上现有批次不重复（网站批次管理页可查）
   - 产出是 MB 级的参数包（不含原始 snp），可以直接传
2. 把 `分享包_5.zip` 上传到本仓库 `uploads/` 目录并发 Pull Request
   （网页操作：Code → uploads → Add file → Upload files → 选 "Create a new branch and start a pull request"）
   - 没有 GitHub 账号：把包发给管理员代传
3. CI 自动校验（列完整性、批次号查重、校验和、fs/fp 物理粗检），PR 上会看到校验结果
4. 合并 PR 后 CI 自动把数据并入网站，几分钟后网站即可见新批次

### 分享包里有什么（透明可查）

- `devices.csv.gz`：每个器件端口的提取参数（fs/fp/Q/kt² 等 34 列）
- `meta.json`：批次信息 + sha256 校验和
- `mapping.json`：对照表条目（EG/FL/AG/面积/PF）

格式版本 `format_version: 1`。打包工具与平台摄取管线同源（同一套提取算法）。

## 管理员操作

```bash
# 全量重建（主机有完整 sqlite + 训练产物时）：见主仓库 使用说明.md §1.4
backend/.venv/bin/python build_web.py    # 在 aln-data-static-web worktree 跑
```
