# setup

| Notebook | What it does |
|---|---|
| `copy_tpch.py` | Creates `${catalog}.${schema}` and the `scratch` volume, then CTAS-copies `region`, `nation`, `supplier`, `part`, `partsupp` in full plus a 1-in-25 customer sample with its `orders` and `lineitem`. Adds table and column comments. Idempotent. |

**Exam objectives:** schema creation, CTAS, managed tables, and table and column comments (governance).

Run with `databricks bundle run setup_tpch -t dev` before any other job.
