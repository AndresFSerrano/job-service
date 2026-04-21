from __future__ import annotations

import json
import subprocess
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from migration_project import _build_sync_database_url


PROJECT_MODULE = "migration_project"


def run_migration_command(*args: str) -> None:
    command = ["db-migration-kit", *args, "--project-module", PROJECT_MODULE]
    print(f"+ {' '.join(command)}", flush=True)
    subprocess.run(command, check=True)


def get_script_directory() -> ScriptDirectory:
    config = Config(str(Path(__file__).resolve().parent / "migrations" / "alembic.ini"))
    return ScriptDirectory.from_config(config)


def get_repo_head_revision() -> str:
    return get_script_directory().get_current_head()


def load_all_snapshots() -> list[dict]:
    snapshots_dir = Path(__file__).resolve().parent / "migrations" / "snapshots"
    snapshots: list[dict] = []
    for path in sorted(snapshots_dir.glob("*.json")):
        snapshots.append(json.loads(path.read_text(encoding="utf-8")))
    return snapshots


def load_snapshot_for_revision(revision: str) -> dict | None:
    for payload in load_all_snapshots():
        if payload.get("alembic_revision") == revision:
            return payload
    return None


def collect_lazy_table_names(snapshots: list[dict]) -> set[str]:
    lazy_tables: set[str] = set()
    for snapshot in snapshots:
        for change in snapshot.get("diff", {}).get("changes", []):
            if change.get("change_type") == "pendiente" and change.get("object_type") == "tabla-lazy":
                object_name = change.get("object_name")
                if object_name:
                    lazy_tables.add(str(object_name))
        for table in snapshot.get("desired_snapshot", {}).get("tables", []):
            if table.get("lazy_materialization") and table.get("name"):
                lazy_tables.add(str(table["name"]))
    return lazy_tables


def collect_column_mismatches(inspector, desired_tables: list[dict], existing_tables: set[str]) -> list[str]:
    mismatches: list[str] = []
    for table in desired_tables:
        table_name = table["name"]
        if table_name not in existing_tables:
            continue
        existing_columns = {
            column["name"]: column
            for column in inspector.get_columns(table_name)
        }
        for desired_column in table.get("columns", []):
            column_name = desired_column["name"]
            existing_column = existing_columns.get(column_name)
            if existing_column is None:
                mismatches.append(f"{table_name}.{column_name}: missing column")
                continue
            desired_nullable = desired_column.get("nullable")
            if desired_nullable is not None and bool(existing_column.get("nullable")) != bool(desired_nullable):
                mismatches.append(
                    f"{table_name}.{column_name}: nullable={existing_column.get('nullable')} expected={desired_nullable}",
                )
    return mismatches


def main() -> int:
    engine = create_engine(_build_sync_database_url())
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())

        current_revision: str | None = None
        if "alembic_version" in table_names:
            with engine.connect() as connection:
                current_revision = connection.execute(
                    text("SELECT version_num FROM alembic_version LIMIT 1"),
                ).scalar_one_or_none()

        if current_revision:
            print(f"Alembic revision actual: {current_revision}", flush=True)
            run_migration_command("upgrade")
            return 0

        repo_head_revision = get_repo_head_revision()
        print(f"Alembic no tiene revision registrada. Head del repo: {repo_head_revision}", flush=True)
        all_snapshots = load_all_snapshots()
        lazy_table_names = collect_lazy_table_names(all_snapshots)
        snapshot = load_snapshot_for_revision(repo_head_revision)
        if snapshot is None:
            print("No se encontro snapshot para la revision head. Se intentara upgrade normal.", flush=True)
            run_migration_command("upgrade")
            return 0

        desired_tables = snapshot.get("desired_snapshot", {}).get("tables", [])
        desired_table_names = {table["name"] for table in desired_tables}
        missing_tables = desired_table_names - table_names
        missing_lazy_tables = sorted(missing_tables & lazy_table_names)
        missing_non_lazy_tables = sorted(missing_tables - lazy_table_names)

        if missing_lazy_tables:
            print(
                f"Se ignoraran tablas lazy no materializadas para decidir el stamp automatico ({', '.join(missing_lazy_tables)}).",
                flush=True,
            )

        if missing_non_lazy_tables:
            print(
                f"Hay tablas faltantes no marcadas como lazy ({', '.join(missing_non_lazy_tables)}). Se intentara upgrade normal.",
                flush=True,
            )

        mismatches = collect_column_mismatches(inspector, desired_tables, table_names)
        if not mismatches:
            print(
                f"La BD ya coincide con el snapshot head sin historial de Alembic. Sellando {repo_head_revision} y continuando con upgrade.",
                flush=True,
            )
            run_migration_command("stamp", "--revision", repo_head_revision)
        else:
            print(
                "No se detecto un estado sellable de forma automatica. Se intentara upgrade normal.",
                flush=True,
            )
            for mismatch in mismatches[:10]:
                print(f" - {mismatch}", flush=True)

        run_migration_command("upgrade")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
