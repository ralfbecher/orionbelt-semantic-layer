# ob-postgres — OrionBelt Semantic Layer Driver for Postgres

## Purpose

PEP 249 DB-API 2.0 driver wrapping `psycopg2-binary = ">=2.9"` that intercepts OBML YAML
queries, compiles them to postgres SQL via the OrionBelt CompilationPipeline
(direct import) or OB REST API (standalone), and executes them natively.

**OB dialect string:** `"postgres"`
**Author:** Ralfo Becher / RALFORION d.o.o. (info@orionbelt.ai)
**License:** Apache 2.0

---

## Module Map

| File | Responsibility |
|---|---|
| `__init__.py` | PEP 249 connect() + module constants |
| `connection.py` | Connection class |
| `cursor.py` | Cursor class — OBML detection + execution |
| `compiler.py` | Direct OB import or REST fallback |
| `exceptions.py` | PEP 249 exception hierarchy |
| `type_codes.py` | PEP 249 type objects |

---

## connect() Parameters

### postgres-specific
| host | str | PostgreSQL host (default: localhost) |
| port | int | PostgreSQL port (default: 5432) |
| dbname | str | Database name |
| user | str | Username |
| password | str | Password |
| sslmode | str | SSL mode (disable/require/verify-full) |

### OrionBelt parameters (same across all vendors)
| Parameter | Default | Description |
|---|---|---|
| ob_model_id | None | Pre-loaded OB model ID |
| ob_model_yaml | None | OBML YAML string to load |
| ob_model_file | None | Path to .obml.yaml file |
| ob_api_url | http://localhost:8000 | OB REST API URL (standalone mode only) |
| ob_timeout | 30 | HTTP timeout in seconds |

---

## Compiler Bridge (compiler.py)

Auto-detects whether OB core is importable (same process as orionbelt-api)
or whether to fall back to REST:

```python
def compile_obml(obml: dict, model, dialect: str) -> str:
    try:
        # Direct call — zero overhead, used when embedded in orionbelt-api
        from orionbelt.compiler.pipeline import CompilationPipeline
        result = CompilationPipeline().compile(obml, model, dialect)
        return result.sql
    except ImportError:
        # Standalone mode — call REST API
        return _compile_via_rest(obml, dialect)
```

---

## Vendor-Specific Notes

- psycopg2 uses %s paramstyle; set paramstyle="format" in module globals
- commit() and rollback() are meaningful — Postgres uses real transactions
- Connection default is autocommit=False; set autocommit=True if needed
- orionbelt_1 schema validated against this driver; fan-trap prevention via UNION ALL is critical

---

## Type System

Use psycopg2.extensions type OIDs for description.type_code.
Map common OIDs: 23=INT4, 25=TEXT, 700=FLOAT4, 701=FLOAT8, 1114=TIMESTAMP.

---

## Build Order

Session 1: exceptions.py + type_codes.py + compiler.py + unit tests
Session 2: connection.py + cursor.py (OBML detection, core execute)
Session 3: __init__.py + connect() + integration tests
Session 4: SQLAlchemy dialect (ob+postgres:// URL scheme)

---

## Dependencies

```toml
[project.dependencies]
psycopg2-binary = ">=2.9"
pyyaml = ">=6.0"

[project.optional-dependencies]
sqlalchemy = ["sqlalchemy>=2.0"]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "mypy>=1.10", "ruff>=0.4", "respx>=0.21"]
```
