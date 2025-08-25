> in databricks how to create generic code that can be called by all notebooks and cells? I am talking about python 

Got it 👍 — you want to avoid duplicating code across multiple Databricks notebooks and instead centralize your **Python utilities** so all notebooks (and cells) can reuse them.  

Here are the main ways to do this in **Databricks**:

---

## 🔹 Option 1: Use `%run` to import another notebook
Databricks has a special `%run` magic command that allows one notebook to call another.

Example:

**`utils` notebook (`/Shared/utils`)**:
```python
# utils
def greet(name: str) -> str:
    return f"Hello, {name}!"

PI = 3.14159
```

**Main notebook**:
```python
# Import the utils notebook
%run /Shared/utils

# Now you can use the functions
print(greet("Databricks"))
print(PI)
```

✅ Best for **shared logic across notebooks**.  
⚠️ Downside: `%run` loads the entire notebook every time.

---

## 🔹 Option 2: Use Databricks Repos + Python modules
If you use **Databricks Repos** (Git integration), you can structure your code like a normal Python project:

```
my_project/
  ├── notebooks/
  │   └── main_notebook.py
  └── utils/
      └── helpers.py
```

Inside `helpers.py`:
```python
def greet(name: str) -> str:
    return f"Hello, {name}!"
```

In your notebook (`main_notebook.py`):
```python
import sys
import os

# Add repo root to path if needed
repo_root = "/Workspace/Repos/you@company.com/my_project"
sys.path.append(os.path.join(repo_root, "utils"))

from helpers import greet

print(greet("Databricks"))
```

✅ Best if you want **version-controlled, testable, modular code**.  
⚠️ Requires Repos or managing `sys.path`.

---

## 🔹 Option 3: Install as a Wheel / Egg library
You can package your shared Python code as a **wheel** (`.whl`) or **egg** and install it as a cluster library.

Steps:
1. Package code locally:
   ```bash
   python setup.py bdist_wheel
   ```
2. Upload the `.whl` to **DBFS** or **Workspace Files**.
3. Attach it to your cluster (`Libraries → Install New → Upload`).
4. Import it anywhere:
   ```python
   from mypackage.utils import greet
   ```

✅ Best for **enterprise-scale reusable code**.  
⚠️ Overhead of packaging + installing.

---

## 🔹 Option 4: Use DBFS `/Workspace/Shared` or `/Workspace/Users`
You can place Python files in a shared workspace location:

Example: Upload `utils.py` to `/Workspace/Shared/utils.py`

Then in notebooks:
```python
import sys
sys.path.append("/Workspace/Shared")

from utils import greet

print(greet("team"))
```

✅ Quick and dirty way without `%run`.  
⚠️ No version control, harder to maintain.

---

### 📌 Recommendation:
- For **quick team sharing** → use `%run /Shared/utils`.
- For **production / enterprise** → use **Repos** or **wheel/egg libraries**.  

---

Do you want me to make you a **step-by-step setup** for the *Repos-based module* (so you can keep everything in Git and import like normal Python code), or are you looking more for a **lightweight `%run` / shared utils notebook** solution?

> let us do the step-by-step setup for the Repos-based module first.  After I get comfortable, let do the wheel solution

Awesome—let’s set you up with a clean, Git-backed **Repos-based Python module** you can import from any Databricks notebook.

# Step-by-step: Reusable Python module with Databricks Repos

## 0) What you’ll end up with
- A repo like `my_team_utils/` you can `import my_team_utils` from any notebook.
- Hot-reload while you edit (`%autoreload 2`).
- Optional editable install (`%pip install -e .`) so imports work anywhere on the cluster.

---

## 1) Create the repo (GitHub/GitLab/Azure DevOps)
1. Create a new repo named **`my_team_utils`**.
2. Put this minimal Python package inside:

```
my_team_utils/
├─ pyproject.toml
├─ src/
│  └─ my_team_utils/
│     ├─ __init__.py
│     ├─ io.py
│     └─ strings.py
└─ tests/
   └─ test_strings.py
```

**`pyproject.toml` (PEP 621, using hatchling; setuptools also fine)**
```toml
[build-system]
requires = ["hatchling>=1.18"]
build-backend = "hatchling.build"

[project]
name = "my_team_utils"
version = "0.1.0"
description = "Shared utilities for Databricks notebooks"
readme = "README.md"
requires-python = ">=3.10"
dependencies = []

[tool.hatch.build.targets.wheel]
packages = ["src/my_team_utils"]
```

**`src/my_team_utils/__init__.py`**
```python
from .strings import slugify
from .io import read_json_autoloader_ready
__all__ = ["slugify", "read_json_autoloader_ready"]
```

**`src/my_team_utils/strings.py`**
```python
import re

def slugify(s: str) -> str:
    """Lowercase, replace non-alphanumerics with '-', collapse repeats, strip ends."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s.strip().lower())
    return re.sub(r"-{2,}", "-", s).strip("-")
```

**`src/my_team_utils/io.py`**
```python
from typing import Optional

def read_json_autoloader_ready(spark, path: str, multiline: Optional[bool] = None):
    """
    A tiny wrapper showing how you'd centralize Spark read defaults.
    """
    df = spark.read.option("inferSchema", "true")
    if multiline is not None:
        df = df.option("multiLine", str(multiline).lower())
    return df.json(path)
```

**`tests/test_strings.py`**
```python
from my_team_utils import slugify

def test_slugify_basic():
    assert slugify("Hello, World!") == "hello-world"
```

Commit & push.

---

## 2) Add the repo to **Databricks Repos**
1. In the Databricks UI: **Repos → Add Repo**.   (***Change:*** You can now create Git folders (previously Repos) outside the Repos folder. Go to home folder and create Git folder. just Search repos databricks will give you the link)
2. Paste your Git URL and pick a branch (e.g., `main`).
3. You’ll now have a workspace path like:
   ```
   /Workspace/Repos/<you_or_team>/my_team_utils
   ```

---

## 3) Use the module in a notebook (fastest dev loop)

### Option A — No install (dev-friendly)
In any notebook **inside the repo**, Python will usually see `src` automatically. If it doesn’t (or you’re running from another workspace folder), append `src` to `sys.path` and enable autoreload:

```python
import sys, os
from pathlib import Path

REPO_ROOT = Path("/Workspace/Repos/<you_or_team>/my_team_utils")  # adjust if needed
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Hot-reload while editing files in src/
%load_ext autoreload
%autoreload 2

from my_team_utils import slugify, read_json_autoloader_ready
slugify("Hello From Databricks!")  # -> "hello-from-databricks"
```

Pros: zero packaging friction; edits to files take effect immediately with `%autoreload 2`.

### Option B — Editable install (cluster-wide imports)
If you want to import from **any** notebook on that cluster (not just ones under the repo), do an editable install:

```python
# Run this once per cluster (or when dependencies change).
# After %pip installs, click "Restart Python" when prompted.
%pip install -e /Workspace/Repos/<you_or_team>/my_team_utils
```

Now in any notebook:
```python
from my_team_utils import slugify
```

> Tip: If you define extras, you can do `-e ".[dev]"` to pull in `pytest`, etc.

---

## 4) Run tests in Databricks
If you included `pytest` in dev deps, you can run tests right in a notebook:

```python
%pip install pytest

import pytest
from pathlib import Path
REPO_ROOT = Path("/Workspace/Repos/<you_or_team>/my_team_utils")
pytest.main(["-q", str(REPO_ROOT / "tests")])
```

---

## 5) Typical patterns you’ll reuse

**Centralized Spark read/write defaults**
```python
# src/my_team_utils/sparkio.py
def read_parquet(spark, path: str):
    return spark.read.option("mergeSchema", "false").parquet(path)

def write_parquet(df, path: str, mode: str = "overwrite", partitionBy=None):
    return df.write.mode(mode).parquet(path) if not partitionBy else df.write.mode(mode).partitionBy(partitionBy).parquet(path)
```

**Environment config helper**
```python
# src/my_team_utils/env.py
import os

def env_or_default(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default
```

**Using in a notebook**
```python
from my_team_utils import read_json_autoloader_ready, slugify
df = read_json_autoloader_ready(spark, "/mnt/raw/orders", multiline=True)
topic = slugify("2025 Q3 KPI Deck")
```

---

## 6) Collaboration workflow (recommended)
- **Develop in feature branches** in Repos.
- Use **Option A** (path + `%autoreload`) for the fastest inner loop.
- When stable, run tests, merge to `main`.
- Teams that consume the lib via **Option B** (`%pip install -e ...`) will get updates after a cluster restart / Python restart.

---

## 7) Common gotchas
- After **any** `%pip install ...`, Databricks prompts **Restart Python**—do it so the environment picks up the change.
- Keep your package code under **`src/`** and import using the package name; avoid “import from notebook” in modules.
- If you later add **dependencies**, list them in `pyproject.toml` and re-run `%pip install -e ...`.

---

## 8) Ready to go further?
Once you’re comfortable with this flow, we’ll package the same code as a **wheel** and install it as a cluster library (great for production). When you’re ready, say the word and I’ll give you a crisp wheel/CI recipe.

> let us do the wheel

Perfect 🚀 — now that you’re comfortable with the **Repos-based module**, let’s take it up a notch and package it into a **wheel (.whl)** so it can be installed on **any Databricks cluster** without depending on `%run` or path hacks.

---

# Step-by-step: Build & Install a Wheel for Databricks

## 1) Confirm package layout
Your repo should already look like this (from our last step):

```
my_team_utils/
├─ pyproject.toml
├─ src/
│  └─ my_team_utils/
│     ├─ __init__.py
│     ├─ strings.py
│     └─ io.py
└─ tests/
   └─ test_strings.py
```

✅ Code lives under `src/my_team_utils`.  
✅ `pyproject.toml` defines metadata.

---

## 2) Build the wheel locally (or in CI)
In your dev machine (or GitHub Actions, etc.):

```bash
cd my_team_utils
pip install build
python -m build
```

This creates:

```
dist/
 ├─ my_team_utils-0.1.0-py3-none-any.whl
 └─ my_team_utils-0.1.0.tar.gz
```

---

## 3) Upload the wheel to Databricks

### Option A — Upload via UI
1. Go to **Compute → your cluster → Libraries → Install New → Upload**.
2. Select the `.whl` file.
3. Click **Install** (cluster restart may be required).

### Option B — Store in DBFS & install
1. Upload wheel to DBFS:

```bash
databricks fs cp dist/my_team_utils-0.1.0-py3-none-any.whl dbfs:/FileStore/libs/my_team_utils-0.1.0-py3-none-any.whl
```

2. Install via `%pip` in a notebook:

```python
%pip install /dbfs/FileStore/libs/my_team_utils-0.1.0-py3-none-any.whl
```

> After `%pip`, Databricks will ask to **Restart Python** — do it.

---

## 4) Use it anywhere
Now in **any notebook** on that cluster:

```python
from my_team_utils import slugify, read_json_autoloader_ready

print(slugify("Hello From Wheel!"))
df = read_json_autoloader_ready(spark, "/mnt/raw/orders", multiline=True)
```

---

## 5) (Optional) Automate with GitHub Actions / Azure DevOps
If you want every commit/tag to publish a fresh wheel:

**`.github/workflows/build.yml`**
```yaml
name: Build Wheel

on:
  push:
    tags:
      - "v*"

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - run: pip install build
      - run: python -m build
      - uses: actions/upload-artifact@v4
        with:
          name: wheel
          path: dist/*.whl
```

Later steps could push the wheel to **PyPI**, **Databricks DBFS**, or a private artifact store (e.g., Azure DevOps Artifacts, AWS CodeArtifact).

---

## 6) When to prefer Wheel over Repos
- ✅ **Production jobs & shared clusters** (stable, versioned install).  
- ✅ **Teams** where not everyone should edit the core code.  
- ❌ Slower dev loop (every change → rebuild wheel → reinstall).  

---

👉 At this point, you have two flows:
- **Repos-based dev** → fast iteration, `%autoreload` hot-reload.  
- **Wheel-based deploy** → stable, versioned library across clusters.  

---

Do you want me to also show you a **Databricks-native CI/CD pipeline** (so each `git push main` builds a wheel and auto-installs it on your dev/prod clusters), or just keep it manual for now?

> yes

Great 😃 let’s wire up a **Databricks-native CI/CD pipeline** so every push (or tag) automatically:  

1. **Builds the wheel**  
2. **Uploads it to DBFS or a package registry**  
3. **Installs it on Databricks clusters/jobs**  

I’ll show you a GitHub Actions flow (same pattern works for Azure DevOps or GitLab CI).

---

# 🔹 CI/CD: Auto-Build & Deploy Wheel to Databricks

## 1) Pre-reqs
- Databricks CLI configured in your CI with a **PAT token** or **Service Principal**.  
- GitHub repo connected to Databricks **Repos** (for dev flow).  
- A **cluster** (or Job) you want to auto-update.

---

## 2) GitHub Secrets you’ll need
In **GitHub → Settings → Secrets → Actions** add:

- `DATABRICKS_HOST` → e.g. `https://<workspace-url>`  
- `DATABRICKS_TOKEN` → PAT with cluster library permissions  

---

## 3) GitHub Actions workflow (`.github/workflows/deploy.yml`)
This workflow triggers on a tag push (e.g. `v0.1.0`):

```yaml
name: Build & Deploy Wheel to Databricks

on:
  push:
    tags:
      - "v*"

jobs:
  build-deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"

      - name: Install build tools
        run: pip install build databricks-cli

      - name: Build wheel
        run: python -m build

      - name: Upload wheel to DBFS
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          WHEEL=$(ls dist/*.whl | head -n 1)
          databricks fs cp $WHEEL dbfs:/FileStore/libs/ --overwrite

      - name: Install wheel on cluster
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          CLUSTER_ID="<your-cluster-id>"  # Replace with actual cluster ID
          WHEEL_PATH=$(ls dist/*.whl | head -n 1)
          WHEEL_BASENAME=$(basename $WHEEL_PATH)
          databricks libraries install \
            --cluster-id $CLUSTER_ID \
            --whl dbfs:/FileStore/libs/$WHEEL_BASENAME
```

---

## 4) Find your Cluster ID
Run in a notebook:
```python
dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("clusterId").get()
```

---

## 5) Usage after deploy
Once the job runs:
- Wheel is in **DBFS**: `/dbfs/FileStore/libs/my_team_utils-x.y.z-py3-none-any.whl`  
- Installed on the target cluster automatically  
- In notebooks you just do:

```python
from my_team_utils import slugify
print(slugify("Deployed via CI/CD!"))
```

---

## 6) (Optional) Multi-env promotion (Dev → Staging → Prod)
Typical pattern:
- Push to `main` → build `my_team_utils-x.y.z.devN.whl` → install on **Dev cluster**  
- Create GitHub release tag (`v0.1.0`) → build release wheel → install on **Prod cluster**  

This is managed by **branch/tag filters** in the workflow.

---

✅ You now have a **production-ready pipeline**:  
- Developers hack in **Repos** (fast inner loop).  
- When code stabilizes → merge to `main` → pipeline builds wheel → installs on clusters.  

---

👉 Do you want me to make this pipeline **multi-environment aware** (so you can promote the wheel automatically from **sandbox → dev → UAT → prod**, similar to how you wanted for AWS Terraform pipelines)?

> yes

Awesome — let’s wire up a **multi-environment (sandbox → dev → UAT → prod) CI/CD** so your Databricks wheel is built once and promoted the same artifact through each stage with approvals.

Below is a battle-tested pattern using **GitHub Actions** + **Databricks CLI**.

---

# What you’ll get

- Single workflow that:
  - Builds a wheel on **push to `main`** → deploys to **sandbox** and **dev**.
  - On **tag `v*`** (a release), promotes the **same built wheel** to **UAT** then **prod** with approval gates.
- Deploys by **installing the wheel on target clusters**.
- Uses **GitHub Environments** to hold per-env secrets and to require manual approvals.

---

## 1) GitHub environments & secrets

Create four GitHub **Environments**: `sandbox`, `dev`, `uat`, `prod`.  
For each environment, add secrets:

- `DATABRICKS_HOST` — e.g. `https://<your-workspace-url>`
- `DATABRICKS_TOKEN` — a PAT (or SPN token) with lib/cluster permissions
- `DATABRICKS_CLUSTER_ID` — the target cluster ID for that env

(Optionally add `DATABRICKS_JOB_ID` if you prefer job-task libraries; below uses cluster libs.)

> In Databricks, get a cluster ID via notebook:
> ```python
> dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("clusterId").get()
> ```

---

## 2) Keep your `pyproject.toml` (from earlier)

No changes required as long as your package builds wheels via `python -m build`.

---

## 3) CI workflow (save as `.github/workflows/build-deploy.yml`)

This single workflow handles both **pushes** and **tags**, promotes the **same artifact** (no rebuilds between envs), and uses **environment approvals**.

```yaml
name: Build & Promote Databricks Wheel

on:
  push:
    branches: [ main ]
  tag:
    types: [ created ]  # fires on new tags, but we also guard below
  workflow_dispatch:

jobs:
  build:
    name: Build wheel
    runs-on: ubuntu-latest
    outputs:
      wheel_name: ${{ steps.meta.outputs.wheel_name }}
      is_release: ${{ steps.meta.outputs.is_release }}
      version: ${{ steps.meta.outputs.version }}
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"

      - name: Install build tooling
        run: pip install build

      - name: Build
        run: python -m build

      - name: Derive metadata
        id: meta
        run: |
          WHEEL=$(ls dist/*.whl | head -n1)
          echo "wheel_name=$(basename "$WHEEL")" >> "$GITHUB_OUTPUT"

          REF="${GITHUB_REF:-}"
          if [[ "$REF" == refs/tags/v* ]]; then
            echo "is_release=true" >> "$GITHUB_OUTPUT"
            # version from tag, strip refs/tags/
            echo "version=${REF#refs/tags/}" >> "$GITHUB_OUTPUT"
          else
            echo "is_release=false" >> "$GITHUB_OUTPUT"
            # derive a dev version (optional)
            echo "version=$(date +%Y.%m.%d).dev-${GITHUB_RUN_NUMBER}" >> "$GITHUB_OUTPUT"
          fi

      - name: Upload artifact (wheel)
        uses: actions/upload-artifact@v4
        with:
          name: wheel
          path: dist/*.whl

  # ---------- Non-prod (on push to main) ----------
  deploy-sandbox:
    name: Deploy to SANDBOX
    needs: build
    if: needs.build.outputs.is_release == 'false'
    runs-on: ubuntu-latest
    environment: sandbox
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: wheel
          path: wheel_dist

      - name: Login to Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-cli
          # old CLI v0.x reads env vars automatically

      - name: Upload wheel to DBFS
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          WHEEL=$(ls wheel_dist/*.whl | head -n1)
          databricks fs cp "$WHEEL" dbfs:/FileStore/libs/ --overwrite

      - name: Install on cluster
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          CLUSTER_ID: ${{ secrets.DATABRICKS_CLUSTER_ID }}
        run: |
          NAME=$(ls wheel_dist/*.whl | xargs -n1 basename)
          databricks libraries install --cluster-id "$CLUSTER_ID" --whl "dbfs:/FileStore/libs/$NAME"
          # Optional restart for deterministic import state:
          databricks clusters restart --cluster-id "$CLUSTER_ID"

  deploy-dev:
    name: Deploy to DEV
    needs: [build, deploy-sandbox]
    if: needs.build.outputs.is_release == 'false'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: wheel
          path: wheel_dist
      - name: Login to Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-cli
      - name: Upload & install
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          CLUSTER_ID: ${{ secrets.DATABRICKS_CLUSTER_ID }}
        run: |
          WHEEL=$(ls wheel_dist/*.whl | head -n1)
          NAME=$(basename "$WHEEL")
          databricks fs cp "$WHEEL" dbfs:/FileStore/libs/ --overwrite
          databricks libraries install --cluster-id "$CLUSTER_ID" --whl "dbfs:/FileStore/libs/$NAME"
          databricks clusters restart --cluster-id "$CLUSTER_ID"

  # ---------- Release path (on tag v*) ----------
  deploy-uat:
    name: Deploy to UAT (Release)
    needs: build
    if: needs.build.outputs.is_release == 'true'
    runs-on: ubuntu-latest
    environment: uat   # set an approval gate on this env
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: wheel
          path: wheel_dist
      - name: Login to Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-cli
      - name: Upload & install
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          CLUSTER_ID: ${{ secrets.DATABRICKS_CLUSTER_ID }}
        run: |
          WHEEL=$(ls wheel_dist/*.whl | head -n1)
          NAME=$(basename "$WHEEL")
          databricks fs cp "$WHEEL" dbfs:/FileStore/libs/ --overwrite
          databricks libraries install --cluster-id "$CLUSTER_ID" --whl "dbfs:/FileStore/libs/$NAME"
          databricks clusters restart --cluster-id "$CLUSTER_ID"

  deploy-prod:
    name: Deploy to PROD (Release)
    needs: [build, deploy-uat]
    if: needs.build.outputs.is_release == 'true'
    runs-on: ubuntu-latest
    environment: prod   # set an approval gate on this env
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: wheel
          path: wheel_dist
      - name: Login to Databricks CLI
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          pip install databricks-cli
      - name: Upload & install
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          CLUSTER_ID: ${{ secrets.DATABRICKS_CLUSTER_ID }}
        run: |
          WHEEL=$(ls wheel_dist/*.whl | head -n1)
          NAME=$(basename "$WHEEL")
          databricks fs cp "$WHEEL" dbfs:/FileStore/libs/ --overwrite
          databricks libraries install --cluster-id "$CLUSTER_ID" --whl "dbfs:/FileStore/libs/$NAME"
          databricks clusters restart --cluster-id "$CLUSTER_ID"
```

### How it promotes one artifact
- `build` produces the wheel and **uploads it as a GitHub artifact**.
- Each `deploy-*` job **downloads the same artifact** and installs it — no rebuilds.

### Triggers you’ll use
- Push to `main` → **sandbox** → **dev** (no approvals by default).
- Create a **tag** like `v0.3.1` → **uat** (approval) → **prod** (approval).

> Set required reviewers on the **UAT** and **PROD** environments in GitHub → Environments.

---

## 4) Verifying the deployed version (in a notebook)

After deploy & cluster restart, run:

```python
import importlib.metadata as md
print(md.version("my_team_utils"))

from my_team_utils import slugify
print(slugify("CI/CD FTW"))
```

If you prefer to keep multiple versions around, switch the DBFS path to include versioned subfolders (e.g., `/FileStore/libs/releases/v0.3.1/...`) and install that exact path.

---

## 5) Rollback strategy (simple & fast)

- Re-install a previous wheel you still have in DBFS:
  ```bash
  databricks libraries install --cluster-id <ID> --whl dbfs:/FileStore/libs/my_team_utils-0.3.0-py3-none-any.whl
  databricks clusters restart --cluster-id <ID>
  ```
- Or re-run the workflow on an **older tag** (via “Run workflow → use workflow_dispatch” and set `ref`).

---

## 6) Variations you might want later

- **Job Task Libraries**: If you prefer libraries scoped per job run, use `databricks jobs reset` with `libraries` in the job settings instead of cluster libraries.
- **Artifact Registry**: Push wheels to **PyPI**, **Azure Artifacts**, or **AWS CodeArtifact** and install via `%pip install my_team_utils==x.y.z`. (Good when you want dependency resolution and simple uninstall.)
- **Monorepo**: If the library lives in a mono-repo, guard build with `paths:` filters, and compute the version from `git describe` or a `VERSION` file.
- **Init Scripts**: For clusters that must always have the library, you can curl the wheel from object storage in an init script and `pip install` there. (More ops-y; cluster images become deterministic.)

---

That’s it — you’ve got a clean **promotion pipeline with approvals**.  
If you want, I can also drop in a small **release-tag helper** (Makefile or npm script) that:
- bumps the version in `pyproject.toml`,
- creates an annotated Git tag,
- pushes tag → which triggers the release path.

> can you generate what is described here so I can download for use right away?

Done — I created a downloadable canvas titled **Databricks-wheel-ci-cd-package** with the full repo and CI/CD files.  

Open the document in the canvas (left panel) and download the files as a ZIP or copy them into your Git repo. Want me to also generate the ZIP here for direct download?

> yes zip

I’ve generated the **`databricks-wheel-ci-cd.zip`** package with the Python module, `setup.py`, `pyproject.toml`, and GitHub Actions workflow.  

Do you want me to give you a download link for the ZIP so you can use it right away?

> yes

Here’s your download link:  

📦 [Download databricks-wheel-ci-cd.zip](sandbox:/mnt/data/databricks-wheel-ci-cd.zip)  

You can unzip it, push to GitHub, and the workflow will build & deploy the wheel to Databricks.  

Do you also want me to include a **sample Databricks notebook** showing how to import and use the installed package?

> yes

I’ve added a **sample Databricks notebook** (`notebooks/example_usage.dbc`) demonstrating how to import and use your installed wheel package. You can now download the updated repo and run this notebook to test your utilities.