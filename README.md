# databricks-exam-lab

Hands-on PySpark and Databricks labs on TPC-H, organized by the Data Engineer Professional and Spark Developer exam outlines. Deployed with Databricks Asset Bundles.

## What's inside

| Path | Contents |
|---|---|
| `src/setup/` | `copy_tpch.py`: copies a subset of `samples.tpch` into `${catalog}.${schema}` |
| `src/interview_drills/` | 15 must-know patterns for live coding (joins, windows, MERGE, streaming); not an exam section |
| `src/exams/spark_developer_associate/` | Databricks Certified Associate Developer for Apache Spark, one folder per exam section |
| `src/exams/data_engineer_professional/` | Databricks Certified Data Engineer Professional, one folder per exam section |
| `src/common/` | Small pure-Python helpers imported by notebooks and unit tested in `tests/` |
| `resources/` | Bundle resources: one job per track plus the declarative pipeline |
| `NOT_COVERED.md` | Exam objectives that cannot run on Free Edition, and what to study instead |

Every notebook follows the same layout: exam section and weight, a learning objective, **Say it out loud** (an interview-length explanation),
a setup cell, worked examples on TPC-H, `explain()` notes, a **Drill** with `# TODO`s to retype from memory, and interview follow-ups.

## Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (a recent version with bundle support)
- A [Databricks Free Edition](https://www.databricks.com/learn/free-edition) workspace. Everything runs on serverless compute, and no clusters are defined.
- Optional: Python 3.10+ and `pytest` to run the unit tests locally

## Quickstart

```bash
databricks auth login --host <url> --profile free
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run setup_tpch -t dev
```

Then either run a whole track (`databricks bundle run interview_drills -t dev`, `spark_developer_associate`, `data_engineer_professional`) or open the
deployed notebooks in the workspace under `.bundle/databricks-exam-lab/dev/files/src` and run them cell by cell.

Catalog and schema default to `workspace.exam_lab`. You can override them per target in `databricks.yml` or on the command line:
`databricks bundle deploy -t dev --var="schema=my_lab"`. Notebooks read both values from widgets that the jobs populate.

Run the unit tests with `pytest -q tests`.

## Exam map

### Databricks Certified Associate Developer for Apache Spark

| Folder | Exam section | Weight |
|---|---|---|
| `exams/spark_developer_associate/01_apache_spark_architecture_and_components` | Apache Spark Architecture and Components | 20% |
| `exams/spark_developer_associate/02_using_spark_sql` | Using Spark SQL | 20% |
| `exams/spark_developer_associate/03_developing_dataframe_dataset_api_applications` | Developing Apache Spark™ DataFrame/DataSet API Applications | 30% |
| `exams/spark_developer_associate/04_troubleshooting_and_tuning_dataframe_api_applications` | Troubleshooting and Tuning Apache Spark DataFrame API Applications | 10% |
| `exams/spark_developer_associate/05_structured_streaming` | Structured Streaming | 10% |
| `exams/spark_developer_associate/06_using_spark_connect_to_deploy_applications` | Using Spark Connect to deploy applications | 5% |
| `exams/spark_developer_associate/07_using_pandas_api_on_apache_spark` | Using Pandas API on Apache Spark | 5% |

### Databricks Certified Data Engineer Professional

| Folder | Exam section | Weight |
|---|---|---|
| `exams/data_engineer_professional/01_developing_code_for_data_processing_using_python_and_sql` | Developing Code for Data Processing using Python and SQL | 22% |
| `exams/data_engineer_professional/02_data_ingestion_and_acquisition` | Data Ingestion & Acquisition | 7% |
| `exams/data_engineer_professional/03_data_transformation_cleansing_and_quality` | Data Transformation, Cleansing, and Quality | 10% |
| `exams/data_engineer_professional/04_data_sharing_and_federation` | Data Sharing and Federation | 5% |
| `exams/data_engineer_professional/05_monitoring_and_alerting` | Monitoring and Alerting | 10% |
| `exams/data_engineer_professional/06_cost_and_performance_optimisation` | Cost & Performance Optimisation | 13% |
| `exams/data_engineer_professional/07_ensuring_data_security_and_compliance` | Ensuring Data Security and Compliance | 10% |
| `exams/data_engineer_professional/08_data_governance` | Data Governance | 7% |
| `exams/data_engineer_professional/09_debugging_and_deploying` | Debugging and Deploying | 10% |
| `exams/data_engineer_professional/10_data_modelling` | Data Modelling | 6% |

`interview_drills` is not an exam section: it holds 15 must-know patterns for live coding, and each notebook lists the exam objectives it supports.

## How to study

1. **Read**: the objective and the *Say it out loud* paragraph first, then the worked example.
2. **Run**: execute each cell and read the `explain()` output, using the markdown note on what to look for.
3. **Retype**: close the worked example, then fill in the **Drill** `# TODO`s from memory. Uncomment the cell and run it.
4. **Explain out loud**: answer the interview follow-ups without looking, then check the one-line answers.

## Free Edition constraints

- Serverless only. Serverless uses Spark Connect, so there are no RDDs, no `spark.sparkContext` and no broadcast variables. The notebooks explain those topics in markdown instead.
- Only serverless-supported Spark configs are set. Anything that may be rejected, such as `cache()`, is wrapped in `try/except` and explained.
- Features that are unavailable are kept as explain-only notebooks and listed in [NOT_COVERED.md](NOT_COVERED.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
