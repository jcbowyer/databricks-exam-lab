# common

Small pure-Python helpers with no Spark imports, so `tests/` can unit test them without a cluster.

| Module | Helpers | Used by |
|---|---|---|
| `naming.py` | `strip_tpch_prefix`, `to_snake_case`, `rename_map` | `exams/data_engineer_professional/01_developing_code_for_data_processing_using_python_and_sql/04_python_modules` |
| `transforms.py` | `mask_phone`, `balance_band` | UDF examples, `exams/data_engineer_professional/09_debugging_and_deploying/04_testing` |

**Exam objectives:** Data Engineer Professional: Developing Code for Data Processing using Python and SQL (importing modules into notebooks); Debugging and Deploying (unit testing).

A notebook imports them by adding `src/` to `sys.path`. A notebook's working directory is its own folder, and notebooks sit at
different depths (`src/interview_drills/` vs `src/exams/<track>/<section>/`), so walk up to `src/` instead of hard-coding `../..`:

```python
import os, sys
src_dir = os.getcwd()
while os.path.basename(src_dir) != "src":
    src_dir = os.path.dirname(src_dir)
sys.path.append(src_dir)
from common.naming import rename_map
```
