# Databricks Python Package + Wheel CI/CD

This repository bundle contains everything you need to:

- Develop a small Python package (`my_team_utils`) under `src/` using Git + Databricks Repos.
- Build a wheel and publish it to Databricks DBFS.
- A GitHub Actions pipeline that builds the wheel and promotes the same artifact through environments: `sandbox -> dev -> uat -> prod` with approval gates.
- Small helper scripts for release tagging and version bumping.
- A sample Databricks notebook showing usage of the installed package.

---

## Repository layout

```
my_team_utils/
├─ pyproject.toml
├─ README.md
├─ Makefile
├─ src/
│  └─ my_team_utils/
│     ├─ __init__.py
│     ├─ strings.py
│     └─ io.py
├─ tests/
│  └─ test_strings.py
├─ notebooks/
│  └─ example_usage.dbc
└─ .github/
   └─ workflows/
      └─ build-deploy.yml
```

---

## Sample Databricks Notebook (`notebooks/example_usage.dbc`)

```python
# Databricks notebook source

# Install the wheel (if not already installed)
%pip install /dbfs/FileStore/libs/my_team_utils-0.1.0-py3-none-any.whl

# Restart Python if prompted

# COMMAND ----------

# Import your package
from my_team_utils import slugify, read_json_autoloader_ready

# COMMAND ----------

# Example usage of slugify
text = "Hello Databricks!"
slug = slugify(text)
print(f"Original: {text}, Slugified: {slug}")

# COMMAND ----------

# Example usage of read_json_autoloader_ready (requires a Spark DataFrame path)
# df = read_json_autoloader_ready(spark, "/mnt/raw/orders", multiline=True)
# display(df)
```

