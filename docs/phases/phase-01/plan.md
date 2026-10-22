# Phase 01 — งานประมวลผลข้อมูลตามรอบและชุดข้อมูลให้บริการ: implementation plan

> **For the executor:** REQUIRED SUB-SKILL: `prince-delivery:execute`, which runs
> `prince-core:tdd` on every task. Steps use `- [ ]` checkboxes — tick them as you go.

**Design:** `docs/phases/phase-01/design.md`
**Tests run with:** `.venv/Scripts/python -m pytest` (pytest; on macOS/Linux `.venv/bin/python -m pytest`)
**Execution:** adaptive batch

## Phase & goal

Phase 01 (Back-end, Wave 1) builds the processing job that reads the Kok River surface-water and sediment views in BigQuery, turns their wide rows into one result per measure, picks the latest result per station, sample type and measure, and publishes stations and latest results as a serving dataset folder that replaces the previous one whole.

Approach: a small Python package `pipeline/`, one module per component in the design, each tested with in-memory rows through a fake BigQuery client, so no test needs BigQuery. The command `python -m pipeline --out <folder>` wires them together and is the only place that talks to the real client.

Left to later phases: previous results and percentages, standards, 7-day history (Phase 05); river lines (Phase 09); alerts (Phase 13); scheduled runs and hosting (ADR 002).

## File map

- `pyproject.toml` — pytest configuration only (test path and import root)
- `requirements.txt` — runtime dependency: the BigQuery client library
- `requirements-dev.txt` — runtime dependencies plus pytest
- `.gitignore` — keeps the virtualenv, caches and local serving output out of git
- `pipeline/__init__.py` — marks the package
- `pipeline/__main__.py` — lets `python -m pipeline` run the command
- `pipeline/registry.py` — the measure registry: every known measure column, its sample type and unit
- `pipeline/source.py` — reads both views read-only through a BigQuery-shaped client
- `pipeline/normalize.py` — turns wide rows into one result per measure
- `pipeline/stations.py` — builds the station list from both views
- `pipeline/latest.py` — picks the latest result per station, sample type and measure
- `pipeline/publish.py` — writes the serving dataset and swaps it in whole
- `pipeline/cli.py` — the run command: wiring, exit codes and the run summary
- `tests/__init__.py` — makes `tests.fakes` importable
- `tests/fakes.py` — fake BigQuery client and wide-row builder shared by the tests
- `tests/test_registry.py`, `tests/test_source.py`, `tests/test_normalize.py`, `tests/test_stations.py`, `tests/test_latest.py`, `tests/test_publish.py`, `tests/test_cli.py` — one test file per module

## Tasks

### Task 1 — Python project configuration

**No test:** configuration file — the dependency lists, pytest settings and ignore rules carry no behaviour of their own.

**Files:**
- Create: `pyproject.toml`
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `.gitignore`
- Create: `tests/__init__.py`

**Batch:** risk: low · complexity: low · context: project-config · fresh: no

- [ ] **Step 1 — Write `pyproject.toml`**

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

- [ ] **Step 2 — Write `requirements.txt`**

```
google-cloud-bigquery>=3.25,<4
```

- [ ] **Step 3 — Write `requirements-dev.txt`**

```
-r requirements.txt
pytest>=8,<9
```

- [ ] **Step 4 — Write `.gitignore`**

```
.venv/
__pycache__/
.pytest_cache/
out/
```

- [ ] **Step 5 — Create `tests/__init__.py`** as an empty file.

- [ ] **Step 6 — Verify**

Run: `python -m venv .venv`
Then: `.venv/Scripts/python -m pip install -r requirements-dev.txt`
Expect: ends with `Successfully installed` naming `google-cloud-bigquery` and `pytest`.

Run: `.venv/Scripts/python -m pytest`
Expect: `no tests ran`, exit code 5 — pytest runs and finds the empty `tests/` package.

Run: `git status --short`
Expect: the five files above and nothing under `.venv/`.

- [ ] **Step 7 — Commit**

`chore(pipeline): add python project configuration`

### Task 2 — Measure registry

**Proves:**
- "lead is registered for both views, in mg/L for surface water and mg/kg for sediment"
- "the registry lists every measure column of both views: 28 for surface water, 11 for sediment"

**Design:** Proving it works 3

**Files:**
- Create: `pipeline/__init__.py`
- Create: `pipeline/registry.py`
- Test: `tests/test_registry.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_registry.py`

**Batch:** risk: low · complexity: low · context: pipeline-registry · fresh: no

#### Cycle 1 — one measure, two units

- [ ] **Step 1 — RED: write the failing test**

`tests/test_registry.py`:

```python
from pipeline.registry import SEDIMENT, WATER, parameters_for


def test_lead_unit_differs_between_water_and_sediment():
    units = {
        p.sample_type: p.unit
        for p in parameters_for(WATER) + parameters_for(SEDIMENT)
        if p.code == "lead"
    }
    assert units == {WATER: "mg/L", SEDIMENT: "mg/kg"}
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_registry.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline'`

- [ ] **Step 3 — GREEN: minimal implementation**

Create `pipeline/__init__.py` as an empty file, and `pipeline/registry.py`:

```python
from dataclasses import dataclass

WATER = "water"
SEDIMENT = "sediment"

TABLES = {WATER: "pcd_water_quality", SEDIMENT: "pcd_sediment_quality"}

STATION_COLUMNS = (
    "station_code",
    "location_name",
    "river_name",
    "province",
    "latitude",
    "longitude",
    "sample_date",
    "sample_time",
    "load_timestamp",
)


@dataclass(frozen=True)
class Parameter:
    code: str
    sample_type: str
    column: str
    unit: str

    @property
    def value_column(self):
        return f"{self.column}_value"

    @property
    def qualifier_column(self):
        return f"{self.column}_qualifier"


PARAMETERS = (
    Parameter("lead", WATER, "lead_mg_l", "mg/L"),
    Parameter("lead", SEDIMENT, "lead_mg_kg", "mg/kg"),
)


def parameters_for(sample_type):
    return tuple(p for p in PARAMETERS if p.sample_type == sample_type)
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_registry.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete each line of `PARAMETERS`, the filter in `parameters_for`, and each property, running `.venv/Scripts/python -m pytest tests/test_registry.py` after each.
Expect: deleting either `Parameter(...)` line or the filter breaks the test. `value_column`, `qualifier_column`, `TABLES` and `STATION_COLUMNS` break nothing here — *untested behaviour* in this task, which Task 3's tests exercise as soon as they exist. Leave them, and record that as a deviation if the executor's `tdd` run asks for one.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): add measure registry with per-view units`

#### Cycle 2 — every measure column of both views

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_registry.py`:

```python
def test_registry_covers_every_measure_column_in_both_views():
    assert len(parameters_for(WATER)) == 28
    assert len(parameters_for(SEDIMENT)) == 11
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_registry.py`
Expect: FAIL — `assert 1 == 28`

- [ ] **Step 10 — GREEN: list every measure**

In `pipeline/registry.py`, replace the `PARAMETERS = (...)` block with:

```python
_WATER_MEASURES = (
    ("depth", "depth_m", "m"),
    ("temp_air", "temp_air", "°C"),
    ("temp_water", "temp_water", "°C"),
    ("ph", "ph", ""),
    ("turbidity", "turbidity_ntu", "NTU"),
    ("conductivity", "conductivity_us", "µS/cm"),
    ("salinity", "salinity_ppt", "ppt"),
    ("do", "do_mg_l", "mg/L"),
    ("bod", "bod_mg_l", "mg/L"),
    ("total_coliform", "total_coliform_mpn_100ml", "MPN/100 mL"),
    ("fecal_coliform", "fecal_coliform_mpn_100ml", "MPN/100 mL"),
    ("nh3_n", "nh3_n_mg_l", "mg/L"),
    ("suspended_solids", "suspended_solids_mg_l", "mg/L"),
    ("total_solids", "total_solids_mg_l", "mg/L"),
    ("total_dissolved_solids", "total_dissolved_solids_mg_l", "mg/L"),
    ("hardness", "hardness_mg_l", "mg/L"),
    ("wqi", "wqi", ""),
)

_METALS = (
    "cadmium",
    "chromium_hexavalent",
    "manganese",
    "nickel",
    "lead",
    "zinc",
    "copper",
    "mercury",
    "arsenic",
    "chromium",
    "cyanide",
)

PARAMETERS = (
    tuple(Parameter(code, WATER, column, unit) for code, column, unit in _WATER_MEASURES)
    + tuple(Parameter(metal, WATER, f"{metal}_mg_l", "mg/L") for metal in _METALS)
    + tuple(Parameter(metal, SEDIMENT, f"{metal}_mg_kg", "mg/kg") for metal in _METALS)
)
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_registry.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete each of the three `tuple(...)` terms of `PARAMETERS` in turn, running `.venv/Scripts/python -m pytest tests/test_registry.py` after each.
Expect: every deletion breaks a test. Restore each.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): register every measure column of both views`

### Task 3 — Source reader

**Proves:**
- "the query for either view never selects `sampler_name` or `remark`"
- "a registry column missing from a view fails the read, naming the column"
- "a measure column the registry does not know is reported, not read"
- "any BigQuery failure becomes `SourceUnavailable` carrying the cause"

**Design:** Proving it works 6; Error handling 1; Error handling 2

**Files:**
- Create: `pipeline/source.py`
- Create: `tests/fakes.py`
- Test: `tests/test_source.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_source.py`

**Batch:** risk: medium · complexity: medium · context: pipeline-source · fresh: no

#### Cycle 1 — query never selects personal or free-text columns

- [ ] **Step 1 — RED: write the failing test**

`tests/test_source.py`:

```python
from pipeline.registry import SEDIMENT, WATER
from pipeline.source import build_query


def test_query_never_selects_personal_or_free_text_columns():
    for sample_type in (WATER, SEDIMENT):
        sql = build_query("proj", "ds", sample_type)
        assert "`lead_mg" in sql
        assert "sampler_name" not in sql
        assert "remark" not in sql
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.source'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/source.py`:

```python
from .registry import STATION_COLUMNS, TABLES, parameters_for


def selected_columns(sample_type):
    columns = list(STATION_COLUMNS)
    for parameter in parameters_for(sample_type):
        columns += [parameter.value_column, parameter.qualifier_column]
    return columns


def build_query(project, dataset, sample_type):
    columns = ", ".join(f"`{column}`" for column in selected_columns(sample_type))
    return f"SELECT {columns} FROM `{project}.{dataset}.{TABLES[sample_type]}`"
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete the `for` loop in `selected_columns`, then the `FROM` clause, running `.venv/Scripts/python -m pytest tests/test_source.py` after each.
Expect: deleting the loop breaks the test (no `` `lead_mg `` column). Deleting `FROM ...` breaks nothing — *untested behaviour* here: the fake client finds rows by the table named in the query, so Task 8 cycle 1 breaks without it. Restore it.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): build view queries without personal columns`

#### Cycle 2 — a registry column missing from a view fails the read

- [ ] **Step 8 — RED: write the shared fakes and the failing test**

`tests/fakes.py`:

```python
from pipeline.registry import SEDIMENT, STATION_COLUMNS, TABLES, WATER, parameters_for

PROJECT = "proj"
DATASET = "ds"


def table_id(sample_type):
    return f"{PROJECT}.{DATASET}.{TABLES[sample_type]}"


def full_schema(sample_type, extra=(), drop=()):
    columns = list(STATION_COLUMNS) + ["sampler_name", "remark"]
    for parameter in parameters_for(sample_type):
        columns += [parameter.value_column, parameter.qualifier_column]
    return [column for column in columns + list(extra) if column not in drop]


def wide_row(
    sample_type,
    station_code="KO01",
    sample_date="2026-09-01",
    sample_time="10:00",
    load_timestamp="2026-09-02T00:00:00+00:00",
    **values,
):
    row = {column: None for column in full_schema(sample_type)}
    row.update(
        station_code=station_code,
        location_name=f"name {station_code}",
        river_name="Kok",
        province="Chiang Rai",
        latitude=19.9,
        longitude=99.8,
        sample_date=sample_date,
        sample_time=sample_time,
        load_timestamp=load_timestamp,
        sampler_name="Somchai",
        remark="field remark",
    )
    row.update(values)
    return row


class _Field:
    def __init__(self, name):
        self.name = name


class _Table:
    def __init__(self, names):
        self.schema = [_Field(name) for name in names]


class _Job:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return list(self._rows)


class FakeClient:
    def __init__(self, schemas=None, rows=None, error=None):
        if schemas is None:
            schemas = {table_id(t): full_schema(t) for t in (WATER, SEDIMENT)}
        self.schemas = schemas
        self.rows = rows or {}
        self.error = error
        self.queries = []

    def get_table(self, table_ref):
        if self.error is not None:
            raise self.error
        return _Table(self.schemas[table_ref])

    def query(self, sql):
        self.queries.append(sql)
        for table_ref, rows in self.rows.items():
            if f"`{table_ref}`" in sql:
                return _Job(rows)
        return _Job([])
```

Add to `tests/test_source.py`, and replace its import lines with:

```python
import pytest

from pipeline.registry import SEDIMENT, WATER
from pipeline.source import SourceSchemaError, build_query, read_source
from tests.fakes import DATASET, PROJECT, FakeClient, full_schema, table_id


def test_missing_registry_column_fails_the_read():
    schemas = {
        table_id(WATER): full_schema(WATER, drop=["lead_mg_l_value"]),
        table_id(SEDIMENT): full_schema(SEDIMENT),
    }
    with pytest.raises(SourceSchemaError, match="lead_mg_l_value"):
        read_source(FakeClient(schemas=schemas), PROJECT, DATASET)
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: FAIL — collection error `ImportError: cannot import name 'SourceSchemaError' from 'pipeline.source'`

- [ ] **Step 10 — GREEN: read both views, checking the schema first**

Add to `pipeline/source.py`, below the import line:

```python
from dataclasses import dataclass, field


class SourceSchemaError(Exception):
    """A column the registry needs is missing from a view."""


@dataclass
class SourceData:
    rows: dict
    unknown_columns: list = field(default_factory=list)
```

and at the end of the file:

```python
def read_source(client, project, dataset):
    rows = {}
    for sample_type, table in TABLES.items():
        schema = {f.name for f in client.get_table(f"{project}.{dataset}.{table}").schema}
        missing = [c for c in selected_columns(sample_type) if c not in schema]
        if missing:
            raise SourceSchemaError(f"{table}: missing columns {', '.join(missing)}")
        job = client.query(build_query(project, dataset, sample_type))
        rows[sample_type] = [dict(row.items()) for row in job.result()]
    return SourceData(rows)
```

Move the `from dataclasses ...` line above `from .registry ...` so the standard-library import comes first.

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete the `if missing:` block, then the two `job`/`rows[...]` lines, running `.venv/Scripts/python -m pytest tests/test_source.py` after each.
Expect: deleting `if missing:` breaks the new test. Deleting the query lines breaks nothing here — *untested behaviour* in this task, which Task 8 cycle 1 exercises by reading the rows end to end. Restore them.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): fail the read when a registry column is missing`

#### Cycle 3 — unknown measure columns are reported, not read

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_source.py`:

```python
def test_unknown_measure_columns_are_reported_not_read():
    schemas = {
        table_id(WATER): full_schema(WATER, extra=["selenium_mg_l_value", "selenium_mg_l_qualifier"]),
        table_id(SEDIMENT): full_schema(SEDIMENT),
    }
    data = read_source(FakeClient(schemas=schemas), PROJECT, DATASET)
    assert data.unknown_columns == ["pcd_water_quality.selenium_mg_l_value"]
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: FAIL — `AssertionError: assert [] == ['pcd_water_quality.selenium_mg_l_value']`

- [ ] **Step 17 — GREEN: collect unknown measure columns**

Replace `read_source` in `pipeline/source.py` with:

```python
def read_source(client, project, dataset):
    rows, unknown = {}, []
    for sample_type, table in TABLES.items():
        schema = {f.name for f in client.get_table(f"{project}.{dataset}.{table}").schema}
        missing = [c for c in selected_columns(sample_type) if c not in schema]
        if missing:
            raise SourceSchemaError(f"{table}: missing columns {', '.join(missing)}")
        known = {p.value_column for p in parameters_for(sample_type)}
        unknown += sorted(f"{table}.{c}" for c in schema if c.endswith("_value") and c not in known)
        job = client.query(build_query(project, dataset, sample_type))
        rows[sample_type] = [dict(row.items()) for row in job.result()]
    return SourceData(rows, unknown)
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Delete `and c not in known`, then the whole `unknown +=` line, running `.venv/Scripts/python -m pytest tests/test_source.py` after each.
Expect: both break the new test. Restore them.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): report unknown measure columns`

#### Cycle 4 — a BigQuery failure becomes SourceUnavailable

- [ ] **Step 22 — RED: write the failing test**

Add `SourceUnavailable` to the `from pipeline.source import ...` line of `tests/test_source.py`, and add:

```python
def test_bigquery_failure_becomes_source_unavailable():
    with pytest.raises(SourceUnavailable, match="credentials expired"):
        read_source(FakeClient(error=RuntimeError("credentials expired")), PROJECT, DATASET)
```

- [ ] **Step 23 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: FAIL — collection error `ImportError: cannot import name 'SourceUnavailable' from 'pipeline.source'`

- [ ] **Step 24 — GREEN: wrap every client call**

In `pipeline/source.py`, add below `SourceSchemaError`:

```python
class SourceUnavailable(Exception):
    """BigQuery could not be read."""
```

and replace `read_source` with:

```python
def read_source(client, project, dataset):
    rows, unknown = {}, []
    for sample_type, table in TABLES.items():
        try:
            schema = {f.name for f in client.get_table(f"{project}.{dataset}.{table}").schema}
        except Exception as error:
            raise SourceUnavailable(f"{table}: {error}") from error
        missing = [c for c in selected_columns(sample_type) if c not in schema]
        if missing:
            raise SourceSchemaError(f"{table}: missing columns {', '.join(missing)}")
        known = {p.value_column for p in parameters_for(sample_type)}
        unknown += sorted(f"{table}.{c}" for c in schema if c.endswith("_value") and c not in known)
        try:
            job = client.query(build_query(project, dataset, sample_type))
            rows[sample_type] = [dict(row.items()) for row in job.result()]
        except Exception as error:
            raise SourceUnavailable(f"{table}: {error}") from error
    return SourceData(rows, unknown)
```

- [ ] **Step 25 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_source.py`
Expect: PASS — `4 passed`, output pristine

- [ ] **Step 26 — Verify MINIMAL**

Delete the first `try`/`except` (keep the `schema =` line), then the second, running `.venv/Scripts/python -m pytest tests/test_source.py` after each.
Expect: deleting the first breaks the new test. Deleting the second breaks nothing — *untested behaviour*: a failure during the query itself. Leave it deleted and add a cycle testing `FakeClient` whose `query` raises, then restore it; record the extra cycle as a deviation.

- [ ] **Step 27 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 28 — Commit**

`feat(pipeline): report BigQuery failures as SourceUnavailable`

### Task 4 — Wide rows to results

**Proves:**
- "a wide row becomes one result per reported measure, with value, unit and sample date"
- "a qualifier with no number is published as the qualifier, never as zero"
- "a row with no station code, coordinates or sample date is skipped and counted"

**Design:** Proving it works 2; Proving it works 5; Error handling 4

**Files:**
- Create: `pipeline/normalize.py`
- Test: `tests/test_normalize.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_normalize.py`

**Batch:** risk: low · complexity: medium · context: pipeline-normalize · fresh: no

#### Cycle 1 — one result per reported measure

- [ ] **Step 1 — RED: write the failing test**

`tests/test_normalize.py`:

```python
from pipeline.normalize import to_results
from pipeline.registry import SEDIMENT, WATER
from tests.fakes import wide_row


def test_wide_row_becomes_one_result_per_reported_measure():
    row = wide_row(SEDIMENT, lead_mg_kg_value=12.5)
    results, skipped = to_results([row], SEDIMENT)
    assert skipped == 0
    assert [(r.parameter, r.value, r.qualifier, r.unit, r.sample_date) for r in results] == [
        ("lead", 12.5, None, "mg/kg", "2026-09-01")
    ]
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.normalize'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/normalize.py`:

```python
from dataclasses import dataclass

from .registry import parameters_for


@dataclass(frozen=True)
class Result:
    station_code: str
    sample_type: str
    parameter: str
    value: float | None
    qualifier: str | None
    unit: str
    sample_date: str
    sample_time: str | None
    load_timestamp: str | None


def _text(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def to_results(rows, sample_type):
    results = []
    for row in rows:
        for parameter in parameters_for(sample_type):
            value = row.get(parameter.value_column)
            if value is None:
                continue
            qualifier = row.get(parameter.qualifier_column) or None
            results.append(
                Result(
                    row["station_code"],
                    sample_type,
                    parameter.code,
                    value,
                    qualifier,
                    parameter.unit,
                    _text(row["sample_date"]),
                    row.get("sample_time") or None,
                    _text(row.get("load_timestamp")),
                )
            )
    return results, 0
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete `if value is None: continue`, then the `isoformat` branch of `_text`, running `.venv/Scripts/python -m pytest tests/test_normalize.py` after each.
Expect: deleting the `continue` breaks the test (ten extra metal results with `None`). Deleting the `isoformat` branch breaks nothing — the fake gives dates as text — but BigQuery returns `datetime.date`, so it is *untested behaviour*: leave it deleted, add a cycle passing `sample_date=datetime.date(2026, 9, 1)` and expecting `"2026-09-01"`, restore it, and record the cycle as a deviation.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): turn wide rows into one result per measure`

#### Cycle 2 — a qualifier with no number is kept

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_normalize.py`:

```python
def test_qualifier_without_number_is_kept_not_zeroed():
    row = wide_row(WATER, mercury_mg_l_value=None, mercury_mg_l_qualifier="<")
    results, _ = to_results([row], WATER)
    assert [(r.parameter, r.value, r.qualifier) for r in results] == [("mercury", None, "<")]
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: FAIL — `AssertionError: assert [] == [('mercury', None, '<')]`

- [ ] **Step 10 — GREEN: skip only when both are empty**

In `to_results`, replace:

```python
            value = row.get(parameter.value_column)
            if value is None:
                continue
            qualifier = row.get(parameter.qualifier_column) or None
```

with:

```python
            value = row.get(parameter.value_column)
            qualifier = row.get(parameter.qualifier_column) or None
            if value is None and qualifier is None:
                continue
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete `and qualifier is None`, then the `or None` after the qualifier read, running `.venv/Scripts/python -m pytest tests/test_normalize.py` after each.
Expect: deleting `and qualifier is None` breaks the new test. Deleting `or None` breaks the first test only if a qualifier is an empty string — the fake uses `None`, so it is *untested behaviour*: add a cycle with `lead_mg_kg_qualifier=""` expecting `qualifier is None`, restore it, record the deviation.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): keep qualifiers that carry no number`

#### Cycle 3 — rows missing key fields are skipped and counted

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_normalize.py`:

```python
def test_row_without_station_coordinates_or_date_is_skipped_and_counted():
    rows = [
        wide_row(WATER, lead_mg_l_value=0.01),
        wide_row(WATER, station_code=None, lead_mg_l_value=0.02),
        wide_row(WATER, latitude=None, lead_mg_l_value=0.03),
        wide_row(WATER, sample_date=None, lead_mg_l_value=0.04),
    ]
    results, skipped = to_results(rows, WATER)
    assert skipped == 3
    assert [r.value for r in results] == [0.01]
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: FAIL — `assert 0 == 3`

- [ ] **Step 17 — GREEN: skip and count**

In `pipeline/normalize.py`, add below the imports:

```python
REQUIRED_FIELDS = ("station_code", "latitude", "longitude", "sample_date")
```

and replace the start and end of `to_results`:

```python
def to_results(rows, sample_type):
    results, skipped = [], 0
    for row in rows:
        if any(row.get(name) in (None, "") for name in REQUIRED_FIELDS):
            skipped += 1
            continue
        for parameter in parameters_for(sample_type):
```

```python
    return results, skipped
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_normalize.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Delete each name in `REQUIRED_FIELDS` in turn, and `skipped += 1`, running `.venv/Scripts/python -m pytest tests/test_normalize.py` after each.
Expect: deleting `station_code`, `latitude`, `sample_date` or `skipped += 1` breaks the test. Deleting `longitude` breaks nothing — *untested behaviour*: add a row with `longitude=None` to this cycle's test (expect `skipped == 4`), restore it, record the deviation.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): skip and count rows missing station, coordinates or date`

### Task 5 — Station list

**Proves:**
- "stations from both views are listed once each, ordered by code"
- "a station's details come from its most recent surface-water row"
- "a row with no station code or coordinates adds no station"

**Files:**
- Create: `pipeline/stations.py`
- Test: `tests/test_stations.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_stations.py`

**Batch:** risk: low · complexity: low · context: pipeline-stations · fresh: no

#### Cycle 1 — one station per code across both views

- [ ] **Step 1 — RED: write the failing test**

`tests/test_stations.py`:

```python
from pipeline.registry import SEDIMENT, WATER
from pipeline.stations import merge_stations
from tests.fakes import wide_row


def test_stations_from_both_views_are_listed_once_by_code():
    rows = {
        WATER: [wide_row(WATER, station_code="KO02"), wide_row(WATER, station_code="KO01")],
        SEDIMENT: [wide_row(SEDIMENT, station_code="KO01"), wide_row(SEDIMENT, station_code="KO03")],
    }
    assert [s.code for s in merge_stations(rows)] == ["KO01", "KO02", "KO03"]
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.stations'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/stations.py`:

```python
from dataclasses import dataclass

from .registry import SEDIMENT, WATER


@dataclass(frozen=True)
class Station:
    code: str
    name: str | None
    river: str | None
    province: str | None
    latitude: float
    longitude: float


def merge_stations(rows_by_type):
    stations = {}
    for sample_type in (WATER, SEDIMENT):
        for row in rows_by_type.get(sample_type, []):
            code = row.get("station_code")
            stations.setdefault(
                code,
                Station(
                    code,
                    row.get("location_name"),
                    row.get("river_name"),
                    row.get("province"),
                    row["latitude"],
                    row["longitude"],
                ),
            )
    return [stations[code] for code in sorted(stations)]
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Replace `sorted(stations)` with `stations`, then drop `SEDIMENT` from the loop tuple, running `.venv/Scripts/python -m pytest tests/test_stations.py` after each.
Expect: both break the test. Restore them.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): list stations from both views once by code`

#### Cycle 2 — details come from the most recent surface-water row

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_stations.py`:

```python
def test_water_view_latest_row_gives_the_station_details():
    rows = {
        WATER: [
            wide_row(WATER, sample_date="2026-01-01", location_name="old name"),
            wide_row(WATER, sample_date="2026-06-01", location_name="new name"),
        ],
        SEDIMENT: [wide_row(SEDIMENT, sample_date="2026-09-01", location_name="sediment name")],
    }
    assert merge_stations(rows)[0].name == "new name"
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: FAIL — `AssertionError: assert 'old name' == 'new name'`

- [ ] **Step 10 — GREEN: newest row first within each view**

In `pipeline/stations.py`, add above `merge_stations`:

```python
def _recency(row):
    return (str(row.get("sample_date") or ""), str(row.get("sample_time") or ""))
```

and change the inner loop line to:

```python
        for row in sorted(rows_by_type.get(sample_type, []), key=_recency, reverse=True):
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete `reverse=True`, then the `sample_time` half of `_recency`, running `.venv/Scripts/python -m pytest tests/test_stations.py` after each.
Expect: deleting `reverse=True` breaks the new test. The `sample_time` half breaks nothing — *untested behaviour*: add a cycle with two rows on the same date, the later time carrying the expected name, restore it, record the deviation.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): take station details from the newest surface-water row`

#### Cycle 3 — rows without code or coordinates add no station

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_stations.py`:

```python
def test_row_without_code_or_coordinates_adds_no_station():
    rows = {
        WATER: [
            wide_row(WATER, station_code="KO01"),
            wide_row(WATER, station_code=None),
            wide_row(WATER, station_code="KO02", longitude=None),
        ],
    }
    assert [s.code for s in merge_stations(rows)] == ["KO01"]
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: FAIL — `TypeError: '<' not supported between instances of 'NoneType' and 'str'`

- [ ] **Step 17 — GREEN: skip incomplete rows**

In `merge_stations`, directly after `code = row.get("station_code")`, add:

```python
            if not code or row.get("latitude") is None or row.get("longitude") is None:
                continue
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_stations.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Delete each of the three conditions in turn, running `.venv/Scripts/python -m pytest tests/test_stations.py` after each.
Expect: deleting `not code` or the `longitude` check breaks the test. Deleting the `latitude` check breaks nothing — *untested behaviour*: add a `latitude=None` row to the test, restore it, record the deviation.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): leave out rows without station code or coordinates`

### Task 6 — Latest result selection

**Proves:**
- "the latest result is chosen per sample type, each with its own sample date"
- "of two rows for the same sample, the most recently loaded wins"
- "a later sample time on the same day wins, whether written with `:` or `.`"

**Design:** Proving it works 1; Error handling 5

**Files:**
- Create: `pipeline/latest.py`
- Test: `tests/test_latest.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_latest.py`

**Batch:** risk: low · complexity: low · context: pipeline-latest · fresh: no

#### Cycle 1 — latest per sample type, each with its own date

- [ ] **Step 1 — RED: write the failing test**

`tests/test_latest.py`:

```python
from pipeline.latest import select_latest
from pipeline.normalize import Result


def result(sample_type="water", date="2026-09-01", time="10:00", value=1.0,
           loaded="2026-09-02T00:00:00+00:00", parameter="lead", station="KO01"):
    return Result(station, sample_type, parameter, value, None, "mg/L", date, time, loaded)


def test_latest_is_chosen_per_sample_type_with_its_own_date():
    picked = select_latest([
        result("water", "2026-08-01", value=1.0),
        result("water", "2026-09-01", value=2.0),
        result("sediment", "2026-05-01", value=3.0),
    ])
    assert [(r.sample_type, r.sample_date, r.value) for r in picked] == [
        ("sediment", "2026-05-01", 3.0),
        ("water", "2026-09-01", 2.0),
    ]
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.latest'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/latest.py`:

```python
def select_latest(results):
    best = {}
    for result in results:
        key = (result.station_code, result.sample_type, result.parameter)
        rank = result.sample_date
        if key not in best or rank > best[key][0]:
            best[key] = (rank, result)
    return [best[key][1] for key in sorted(best)]
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete `or rank > best[key][0]`, then replace `sorted(best)` with `best`, running `.venv/Scripts/python -m pytest tests/test_latest.py` after each.
Expect: both break the test. Restore them.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): select the latest result per station, type and measure`

#### Cycle 2 — duplicate sample keeps the most recently loaded row

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_latest.py`:

```python
def test_duplicate_sample_keeps_the_most_recently_loaded_row():
    picked = select_latest([
        result(value=2.0, loaded="2026-09-02T00:00:00+00:00"),
        result(value=1.0, loaded="2026-09-03T00:00:00+00:00"),
    ])
    assert [r.value for r in picked] == [1.0]
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: FAIL — `assert [2.0] == [1.0]`

- [ ] **Step 10 — GREEN: break ties by load time**

In `select_latest`, replace `rank = result.sample_date` with:

```python
        rank = (result.sample_date, result.load_timestamp or "")
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete `or ""` from the rank, running `.venv/Scripts/python -m pytest tests/test_latest.py`.
Expect: nothing breaks — *untested behaviour*: a result with no `load_timestamp` compared against one with it raises `TypeError`. Leave it deleted, add a cycle with `loaded=None` on one of two same-date results expecting the one with a timestamp to win, restore it, record the deviation.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): keep the most recently loaded duplicate sample`

#### Cycle 3 — later sample time on the same day wins

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_latest.py`:

```python
def test_later_sample_time_on_the_same_day_wins_whatever_its_separator():
    picked = select_latest([
        result(time="10.15", value=2.0, loaded="2026-09-02T00:00:00+00:00"),
        result(time="9:30", value=1.0, loaded="2026-09-03T00:00:00+00:00"),
    ])
    assert [r.value for r in picked] == [2.0]
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: FAIL — `assert [1.0] == [2.0]`

- [ ] **Step 17 — GREEN: rank by normalised sample time**

In `pipeline/latest.py`, add above `select_latest`:

```python
def _time_key(sample_time):
    if not sample_time:
        return ""
    return sample_time.strip().replace(".", ":").zfill(5)
```

and replace the rank line with:

```python
        rank = (result.sample_date, _time_key(result.sample_time), result.load_timestamp or "")
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_latest.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Delete `.replace(".", ":")`, then `.zfill(5)`, then the `if not sample_time` branch, running `.venv/Scripts/python -m pytest tests/test_latest.py` after each.
Expect: deleting `.replace` or `.zfill` breaks the new test. Deleting the `if not sample_time` branch breaks nothing in this file — *untested behaviour*: add a cycle with `time=None` against `time="08:00"` on the same date expecting `08:00` to win, restore it, record the deviation.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): order same-day samples by normalised sample time`

### Task 7 — Publisher

**Proves:**
- "publishing writes stations, latest results and a run file carrying the format version"
- "publishing replaces the previous set and leaves nothing else behind"
- "a write that fails keeps the previous set byte for byte and removes its staging folder"
- "the same input gives byte-identical stations and latest files whatever its order"
- "a folder that is not a serving set is never replaced"

**Design:** Proving it works 8; Proving it works 10; Error handling 6

**Files:**
- Create: `pipeline/publish.py`
- Test: `tests/test_publish.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_publish.py`

**Batch:** risk: high · complexity: medium · context: pipeline-publish · fresh: no

#### Cycle 1 — three files, with the format version

- [ ] **Step 1 — RED: write the failing test**

`tests/test_publish.py`:

```python
import json

import pytest

from pipeline import publish as publish_module
from pipeline.normalize import Result
from pipeline.publish import SCHEMA_VERSION, publish
from pipeline.stations import Station

STATIONS = [Station("KO01", "name KO01", "Kok", "Chiang Rai", 19.9, 99.8)]
LATEST = [Result("KO01", "water", "lead", 0.02, None, "mg/L", "2026-09-01", "10:00", "2026-09-02T00:00:00+00:00")]
SEDIMENT_LEAD = Result("KO01", "sediment", "lead", 9.0, None, "mg/kg", "2026-05-01", None, None)
RUN = {
    "generated_at": "2026-10-22T00:00:00+00:00",
    "rows_read": {"water": 1, "sediment": 0},
    "rows_skipped": 0,
    "unknown_columns": [],
}


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_publish_writes_stations_latest_and_run_files(tmp_path):
    out = tmp_path / "serving"
    publish(out, STATIONS, LATEST, RUN)
    assert read_json(out / "run.json")["schema_version"] == SCHEMA_VERSION
    assert read_json(out / "latest.json") == [{
        "station_code": "KO01", "sample_type": "water", "parameter": "lead", "value": 0.02,
        "qualifier": None, "unit": "mg/L", "sample_date": "2026-09-01", "sample_time": "10:00",
    }]
    assert read_json(out / "stations.json")[0]["code"] == "KO01"
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.publish'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/publish.py`:

```python
import json
from dataclasses import asdict
from pathlib import Path

SCHEMA_VERSION = 1

LATEST_FIELDS = (
    "station_code",
    "sample_type",
    "parameter",
    "value",
    "qualifier",
    "unit",
    "sample_date",
    "sample_time",
)


def _write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def publish(out_dir, stations, latest, run_info):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "stations.json", [asdict(s) for s in stations])
    _write(out / "latest.json", [{f: getattr(r, f) for f in LATEST_FIELDS} for r in latest])
    _write(out / "run.json", {"schema_version": SCHEMA_VERSION, **run_info})
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete each of the three `_write` calls in turn, then `"schema_version": SCHEMA_VERSION`, running `.venv/Scripts/python -m pytest tests/test_publish.py` after each.
Expect: every deletion breaks the test. Restore each.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): write stations, latest results and run file`

#### Cycle 2 — replace the previous set whole

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_publish.py`:

```python
def test_publish_replaces_the_previous_set_and_leaves_nothing_behind(tmp_path):
    out = tmp_path / "serving"
    publish(out, STATIONS, LATEST, RUN)
    (out / "stale.json").write_text("{}", encoding="utf-8")
    publish(out, STATIONS, LATEST, RUN)
    assert sorted(p.name for p in out.iterdir()) == ["latest.json", "run.json", "stations.json"]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["serving"]
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: FAIL — `AssertionError` listing `'stale.json'` among the files in `serving`

- [ ] **Step 10 — GREEN: stage, then swap**

In `pipeline/publish.py`, add `import shutil` and `import tempfile` to the imports, and replace `publish` with:

```python
def publish(out_dir, stations, latest, run_info):
    out = Path(out_dir)
    out.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{out.name}-new-", dir=out.parent))
    _write(staging / "stations.json", [asdict(s) for s in stations])
    _write(staging / "latest.json", [{f: getattr(r, f) for f in LATEST_FIELDS} for r in latest])
    _write(staging / "run.json", {"schema_version": SCHEMA_VERSION, **run_info})
    previous = out.parent / f".{out.name}-previous"
    if previous.exists():
        shutil.rmtree(previous)
    if out.exists():
        out.rename(previous)
    staging.rename(out)
    shutil.rmtree(previous, ignore_errors=True)
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete the final `shutil.rmtree(previous, ...)`, then `if out.exists(): out.rename(previous)`, then the `if previous.exists()` block, running `.venv/Scripts/python -m pytest tests/test_publish.py` after each.
Expect: deleting the final `rmtree` breaks the new test (a `.serving-previous` folder is left). Deleting the `out.rename` makes `staging.rename(out)` fail on the second publish — the test breaks. The `if previous.exists()` block breaks nothing — *untested behaviour*: a `.serving-previous` left by a crash. Add a cycle creating that folder before publishing and asserting the publish succeeds and removes it, restore the block, record the deviation.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): swap in the new serving set whole`

#### Cycle 3 — a failed write keeps the previous set

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_publish.py`:

```python
def test_failed_write_keeps_the_previous_set_byte_for_byte(tmp_path, monkeypatch):
    out = tmp_path / "serving"
    publish(out, STATIONS, LATEST, RUN)
    before = {p.name: p.read_bytes() for p in out.iterdir()}
    real_write = publish_module._write

    def failing_write(path, data):
        if path.name == "latest.json":
            raise OSError("disk full")
        real_write(path, data)

    monkeypatch.setattr(publish_module, "_write", failing_write)
    with pytest.raises(OSError, match="disk full"):
        publish(out, STATIONS, LATEST, RUN)
    assert {p.name: p.read_bytes() for p in out.iterdir()} == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["serving"]
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: FAIL — the last assertion lists a `.serving-new-...` folder beside `serving`

- [ ] **Step 17 — GREEN: remove the staging folder on failure**

In `publish`, wrap the three `_write` calls:

```python
    try:
        _write(staging / "stations.json", [asdict(s) for s in stations])
        _write(staging / "latest.json", [{f: getattr(r, f) for f in LATEST_FIELDS} for r in latest])
        _write(staging / "run.json", {"schema_version": SCHEMA_VERSION, **run_info})
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Delete the `shutil.rmtree(staging, ...)` line, then the `raise`, running `.venv/Scripts/python -m pytest tests/test_publish.py` after each.
Expect: both break the new test. Restore them.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): keep the previous serving set when a write fails`

#### Cycle 4 — identical input gives identical files

- [ ] **Step 22 — RED: write the failing test**

Add to `tests/test_publish.py`:

```python
def test_rerun_with_same_input_gives_identical_data_files(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    publish(first, STATIONS, LATEST + [SEDIMENT_LEAD], RUN)
    publish(second, STATIONS, [SEDIMENT_LEAD] + LATEST, dict(RUN, generated_at="2026-10-23T00:00:00+00:00"))
    for name in ("stations.json", "latest.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
```

- [ ] **Step 23 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: FAIL — `AssertionError` comparing the two `latest.json` byte strings

- [ ] **Step 24 — GREEN: sort what is written**

In `publish`, replace the first two `_write` calls with:

```python
        _write(staging / "stations.json", [asdict(s) for s in sorted(stations, key=lambda s: s.code)])
        entries = [{f: getattr(r, f) for f in LATEST_FIELDS} for r in latest]
        _write(
            staging / "latest.json",
            sorted(entries, key=lambda e: (e["station_code"], e["sample_type"], e["parameter"])),
        )
```

- [ ] **Step 25 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: PASS — `4 passed`, output pristine

- [ ] **Step 26 — Verify MINIMAL**

Remove the `sorted(...)` around `entries`, then around `stations`, running `.venv/Scripts/python -m pytest tests/test_publish.py` after each.
Expect: removing it around `entries` breaks the new test. Around `stations` breaks nothing — one station only. *Untested behaviour*: add a second station to one call in reverse order, restore the sort, record the deviation.

- [ ] **Step 27 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 28 — Commit**

`feat(pipeline): write serving files in a stable order`

#### Cycle 5 — never replace a folder that is not a serving set

- [ ] **Step 29 — RED: write the failing test**

Add to `tests/test_publish.py`:

```python
def test_refuses_to_replace_a_folder_that_is_not_a_serving_set(tmp_path):
    out = tmp_path / "documents"
    out.mkdir()
    (out / "thesis.docx").write_text("precious", encoding="utf-8")
    with pytest.raises(FileExistsError, match="not a serving dataset"):
        publish(out, STATIONS, LATEST, RUN)
    assert (out / "thesis.docx").read_text(encoding="utf-8") == "precious"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["documents"]
```

- [ ] **Step 30 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: FAIL — `Failed: DID NOT RAISE <class 'FileExistsError'>`

- [ ] **Step 31 — GREEN: refuse before staging anything**

At the top of `publish`, directly after `out = Path(out_dir)`, add:

```python
    if out.exists() and not (out / "run.json").is_file():
        raise FileExistsError(f"{out} exists and is not a serving dataset (no run.json); refusing to replace it")
```

- [ ] **Step 32 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_publish.py`
Expect: PASS — `5 passed`, output pristine

- [ ] **Step 33 — Verify MINIMAL**

Delete `out.exists() and`, then `not (out / "run.json").is_file()`, running `.venv/Scripts/python -m pytest tests/test_publish.py` after each.
Expect: deleting `out.exists() and` breaks the first test (a new folder has no `run.json`). Deleting the `run.json` check makes every second publish raise. Restore both.

- [ ] **Step 34 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 35 — Commit**

`feat(pipeline): refuse to replace a folder that is not a serving set`

### Task 8 — Run command

**Proves:**
- "a run publishes latest results for each sample type it has, skips unknown measure columns, and records skipped rows and unknown columns in both its summary and `run.json`"
- "an unreadable source exits 1, prints the cause and leaves the previous set unchanged"
- "a registry column missing from a view exits 1 and leaves the previous set unchanged"
- "missing credentials exit 1 with the cause and publish nothing"

**Design:** Proving it works 4; Proving it works 7; Proving it works 9; Error handling 1; Error handling 2; Error handling 3

**Files:**
- Create: `pipeline/cli.py`
- Create: `pipeline/__main__.py`
- Test: `tests/test_cli.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_cli.py`

**Batch:** risk: medium · complexity: medium · context: pipeline-cli · fresh: no

#### Cycle 1 — end-to-end run with a summary

- [ ] **Step 1 — RED: write the failing test**

`tests/test_cli.py`:

```python
import json

from pipeline.cli import main
from pipeline.registry import SEDIMENT, WATER
from tests.fakes import FakeClient, full_schema, table_id, wide_row


def args(out):
    return ["--out", str(out), "--project", "proj", "--dataset", "ds"]


def client_with(rows, schemas=None, error=None):
    client = FakeClient(schemas=schemas, rows={table_id(t): r for t, r in rows.items()}, error=error)
    return lambda project: client


def snapshot(folder):
    return {p.name: p.read_bytes() for p in folder.iterdir()}


def test_run_publishes_latest_results_and_reports_what_it_skipped(tmp_path, capsys):
    schemas = {
        table_id(WATER): full_schema(WATER, extra=["selenium_mg_l_value"]),
        table_id(SEDIMENT): full_schema(SEDIMENT),
    }
    rows = {
        WATER: [
            wide_row(WATER, station_code="KO01", lead_mg_l_value=0.02),
            wide_row(WATER, station_code=None, lead_mg_l_value=0.5),
        ],
        SEDIMENT: [wide_row(SEDIMENT, station_code="KO02", lead_mg_kg_value=12.5)],
    }
    code = main(args(tmp_path / "serving"), client_factory=client_with(rows, schemas=schemas))
    assert code == 0
    latest = json.loads((tmp_path / "serving" / "latest.json").read_text(encoding="utf-8"))
    assert [(e["station_code"], e["sample_type"], e["value"], e["unit"]) for e in latest] == [
        ("KO01", "water", 0.02, "mg/L"),
        ("KO02", "sediment", 12.5, "mg/kg"),
    ]
    assert all(e["parameter"] != "selenium" for e in latest)
    summary = capsys.readouterr().out
    assert "skipped 1 rows" in summary
    assert "pcd_water_quality.selenium_mg_l_value" in summary
    run = json.loads((tmp_path / "serving" / "run.json").read_text(encoding="utf-8"))
    assert run["rows_skipped"] == 1
    assert run["unknown_columns"] == ["pcd_water_quality.selenium_mg_l_value"]
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: FAIL — collection error `ModuleNotFoundError: No module named 'pipeline.cli'`

- [ ] **Step 3 — GREEN: minimal implementation**

`pipeline/cli.py`:

```python
import argparse
from datetime import datetime, timezone

from .latest import select_latest
from .normalize import to_results
from .publish import publish
from .source import read_source
from .stations import merge_stations

DEFAULT_PROJECT = "envilink-data-platform"
DEFAULT_DATASET = "standardized__kok_river"


def _parser():
    parser = argparse.ArgumentParser(prog="python -m pipeline", description="Publish the Kok River serving dataset.")
    parser.add_argument("--out", required=True, help="serving dataset folder to create or replace")
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    return parser


def _bigquery_client(project):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main(argv=None, client_factory=_bigquery_client, now=_utcnow):
    args = _parser().parse_args(argv)
    client = client_factory(args.project)
    source = read_source(client, args.project, args.dataset)
    results, skipped = [], 0
    for sample_type, rows in source.rows.items():
        found, dropped = to_results(rows, sample_type)
        results += found
        skipped += dropped
    stations = merge_stations(source.rows)
    latest = select_latest(results)
    publish(
        args.out,
        stations,
        latest,
        {
            "generated_at": now(),
            "rows_read": {t: len(r) for t, r in source.rows.items()},
            "rows_skipped": skipped,
            "unknown_columns": source.unknown_columns,
        },
    )
    unknown = ", ".join(source.unknown_columns) or "none"
    print(
        f"published {len(stations)} stations and {len(latest)} latest results to {args.out}; "
        f"skipped {skipped} rows; unknown columns: {unknown}"
    )
    return 0
```

`pipeline/__main__.py`:

```python
from .cli import main

raise SystemExit(main())
```

- [ ] **Step 4 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: PASS — `1 passed`, output pristine

- [ ] **Step 5 — Verify MINIMAL**

Delete `skipped += dropped`, then `{unknown}` from the summary, then the `publish(...)` call, running `.venv/Scripts/python -m pytest tests/test_cli.py` after each.
Expect: every deletion breaks the test. `_bigquery_client` and `pipeline/__main__.py` are never run by a test — *untested behaviour* by design: they are the real BigQuery path, checked by the manual run in *Open questions*. Leave them.

- [ ] **Step 6 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 7 — Commit**

`feat(pipeline): add the run command`

#### Cycle 2 — an unreadable source exits 1 and keeps the previous set

- [ ] **Step 8 — RED: write the failing test**

Add to `tests/test_cli.py`:

```python
def test_unreadable_source_exits_nonzero_and_keeps_the_previous_set(tmp_path, capsys):
    out = tmp_path / "serving"
    assert main(args(out), client_factory=client_with({WATER: [wide_row(WATER, lead_mg_l_value=0.02)]})) == 0
    before = snapshot(out)
    code = main(args(out), client_factory=client_with({}, error=RuntimeError("credentials expired")))
    assert code == 1
    assert snapshot(out) == before
    assert "credentials expired" in capsys.readouterr().err
```

- [ ] **Step 9 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: FAIL — `pipeline.source.SourceUnavailable: pcd_water_quality: credentials expired`

- [ ] **Step 10 — GREEN: catch an unavailable source**

In `pipeline/cli.py`, add `import sys` to the imports, change `from .source import read_source` to `from .source import SourceUnavailable, read_source`, and replace the two lines

```python
    client = client_factory(args.project)
    source = read_source(client, args.project, args.dataset)
```

with:

```python
    try:
        client = client_factory(args.project)
        source = read_source(client, args.project, args.dataset)
    except SourceUnavailable as error:
        print(f"run failed: {error}", file=sys.stderr)
        return 1
```

- [ ] **Step 11 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: PASS — `2 passed`, output pristine

- [ ] **Step 12 — Verify MINIMAL**

Delete the `print(...)` line, then `return 1`, running `.venv/Scripts/python -m pytest tests/test_cli.py` after each.
Expect: both break the new test. Restore them.

- [ ] **Step 13 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 14 — Commit**

`feat(pipeline): exit 1 and keep the previous set when BigQuery is unreadable`

#### Cycle 3 — a missing registry column exits 1 and keeps the previous set

- [ ] **Step 15 — RED: write the failing test**

Add to `tests/test_cli.py`:

```python
def test_missing_registry_column_exits_nonzero_and_keeps_the_previous_set(tmp_path, capsys):
    out = tmp_path / "serving"
    assert main(args(out), client_factory=client_with({WATER: [wide_row(WATER, lead_mg_l_value=0.02)]})) == 0
    before = snapshot(out)
    schemas = {
        table_id(WATER): full_schema(WATER, drop=["lead_mg_l_value"]),
        table_id(SEDIMENT): full_schema(SEDIMENT),
    }
    code = main(args(out), client_factory=client_with({}, schemas=schemas))
    assert code == 1
    assert snapshot(out) == before
    assert "lead_mg_l_value" in capsys.readouterr().err
```

- [ ] **Step 16 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: FAIL — `pipeline.source.SourceSchemaError: pcd_water_quality: missing columns lead_mg_l_value`

- [ ] **Step 17 — GREEN: catch a schema error too**

In `pipeline/cli.py`, change the import to `from .source import SourceSchemaError, SourceUnavailable, read_source` and the handler line to:

```python
    except (SourceUnavailable, SourceSchemaError) as error:
```

- [ ] **Step 18 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: PASS — `3 passed`, output pristine

- [ ] **Step 19 — Verify MINIMAL**

Remove `SourceSchemaError` from the `except` tuple, running `.venv/Scripts/python -m pytest tests/test_cli.py`.
Expect: the new test breaks. Restore it.

- [ ] **Step 20 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 21 — Commit**

`feat(pipeline): exit 1 when a view is missing a registry column`

#### Cycle 4 — missing credentials exit 1 and publish nothing

- [ ] **Step 22 — RED: write the failing test**

Add to `tests/test_cli.py`:

```python
def test_missing_credentials_exit_nonzero_and_publish_nothing(tmp_path, capsys):
    def no_credentials(project):
        raise RuntimeError("default credentials not found")

    assert main(args(tmp_path / "serving"), client_factory=no_credentials) == 1
    assert "default credentials not found" in capsys.readouterr().err
    assert not (tmp_path / "serving").exists()
```

- [ ] **Step 23 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: FAIL — `RuntimeError: default credentials not found`

- [ ] **Step 24 — GREEN: treat a failed client as an unavailable source**

In `main`, replace `client = client_factory(args.project)` with:

```python
        try:
            client = client_factory(args.project)
        except Exception as error:
            raise SourceUnavailable(str(error)) from error
```

- [ ] **Step 25 — Verify GREEN**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: PASS — `4 passed`, output pristine

- [ ] **Step 26 — Verify MINIMAL**

Delete the inner `try`/`except`, keeping `client = client_factory(args.project)`, running `.venv/Scripts/python -m pytest tests/test_cli.py`.
Expect: the new test breaks. Restore it.

- [ ] **Step 27 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 28 — Commit**

`feat(pipeline): exit 1 when BigQuery credentials are missing`

### Task 9 — No personal data in the published files

**Pins existing behaviour:** the registry and the normaliser already carry only registered measure columns and station fields, so a published file holds no `sampler_name` or `remark`; this task pins that end to end.

**Proves:**
- "a run whose rows carry `sampler_name` and `remark` publishes neither"

**Design:** Proving it works 6

**Files:**
- Test: `tests/test_cli.py`

**Targeted:** `.venv/Scripts/python -m pytest tests/test_cli.py`

**Batch:** risk: medium · complexity: low · context: pipeline-cli · fresh: no

#### Cycle 1 — personal and free-text fields never reach a file

- [ ] **Step 1 — RED: write the test**

Add to `tests/test_cli.py`:

```python
def test_published_files_carry_no_sampler_name_or_remark(tmp_path):
    out = tmp_path / "serving"
    assert main(args(out), client_factory=client_with({WATER: [wide_row(WATER, lead_mg_l_value=0.02)]})) == 0
    text = "".join(p.read_text(encoding="utf-8") for p in out.iterdir())
    assert "Somchai" not in text
    assert "field remark" not in text
```

- [ ] **Step 2 — Verify RED**

Run: `.venv/Scripts/python -m pytest tests/test_cli.py`
Expect: PASS — this pins behaviour Tasks 4, 5 and 7 already deliver, which is `tdd`'s refactor case. Confirm it is load-bearing instead: in `pipeline/stations.py` temporarily add `remark=row.get("remark")` as an extra field to the `Station(...)` call and a matching `remark: str | None = None` field on `Station`, run the targeted command, and expect FAIL on `assert "field remark" not in text`. Revert the change.

- [ ] **Step 3 — Gate**

Run: `.venv/Scripts/python -m pytest`
Expect: PASS, output pristine

- [ ] **Step 4 — Commit**

`test(pipeline): pin that published files carry no personal data`

## Assumptions & answers

- **Readiness gate.** The user re-ran `phase-plan` instead of answering the first readiness question, so the six blocking marks below are planned against stated assumptions rather than answers. Each is cheap to reverse and named in the task that relies on it; the design keeps its marks.
  - *`sample_time` is a STRING of unknown form* — assumed `H:MM`, `HH:MM`, `HH.MM` or `HH:MM:SS`, local time, possibly empty; empty sorts before any time on the same day (Task 6 cycle 3).
  - *"Latest" basis* — `sample_date`, then `sample_time`, then `load_timestamp` (Task 6). `data_date` is not used.
  - *Qualifier values unknown* — qualifiers are carried through as text and never interpreted (Task 4). No value list is needed to plan.
  - *WQI* — published as a measure with no unit (Task 2). Removing it is one registry line.
  - *Station details disagree between views* — the most recent surface-water row wins, then sediment (Task 5).
  - *Serving file format, to agree with Dev 1* — planned as version 1: `stations.json` (code, name, river, province, latitude, longitude), `latest.json` (station_code, sample_type, parameter, value, qualifier, unit, sample_date, sample_time), `run.json` (schema_version, generated_at, rows_read, rows_skipped, unknown_columns). A change Dev 1 asks for before Phase 02 is an amendment to Task 7; after it, the version goes up.
- **Ground.** No dependency manifest or CI existed; Task 1 creates them, so the test command is the one Task 1 sets up: `.venv/Scripts/python -m pytest`. The repository holds no code, so there was no source file to open and no existing test to match. `pytest` and `google-cloud-bigquery` are not installed on this machine (checked with `python -I -m pytest --version` and an import). The view columns come from `bq show --format=prettyjson` on both views; no rows were read.
- **Design count corrected.** The design says 27 surface-water and 12 sediment measures. The schema has 28 surface-water measures (17 general plus 11 metals) and 11 sediment metals; Task 2 registers 28 and 11. `design.md` is left as it is.
- **Units not in the column name** are assumed: air and water temperature `°C`, conductivity `µS/cm`, pH and WQI unitless.
- **Credentials.** The real run uses Application Default Credentials, set up on this machine with `gcloud auth application-default login`; no key is written anywhere.
- **Publish safety, beyond the design.** Task 7 cycle 5 refuses to replace a folder that has no `run.json`, so a mistyped `--out` cannot delete an unrelated folder. It traces to the publisher's "replace the previous serving set".
- Coverage review: independent — 3 round(s), no findings left.
- The coverage reviewers were given the design and the draft plan as two file paths to read, rather than their text pasted into the brief; each was told to open no other file and was given none of the planner's reasoning.

## Open questions

- **Manual run against BigQuery, after `execute`.** `.venv/Scripts/python -m pipeline --out out/serving` once, by a person with read access; check the summary's row counts and unknown columns, and spot-check one station's latest values against the view. This also exercises `_bigquery_client` and `pipeline/__main__.py`, which no test runs.
- **Query billing project.** The client bills queries to `--project` (`envilink-data-platform`). If the runner cannot create jobs there, a separate billing project option is needed.
- **Swap gap.** If renaming the staging folder into place fails after the old set was moved aside, `out` is briefly missing until the next run; the old set is still at `.<name>-previous`.
- **`round`** — meaning, and whether it should take part in ordering (design mark, non-blocking).
- **Code outside this repository** that already reads these views (design mark, non-blocking).
- **ADR 002** still `proposed` though Python is confirmed — re-run `system-design` (design mark).
- **Size of `latest.json` against the 5-second page target** — unknown until the manual run gives row counts (design mark).
- **Threat model** — run `/prince-guardrails:threat-model` before public release (design mark).
- **Local output folder and how the web reads it** — this plan uses `out/serving`, ignored by git; wiring is Phase 04 (design mark).
- **Calibration** — the roadmap estimated 6–8 tasks; this plan has 9 (one configuration task, one pin).
