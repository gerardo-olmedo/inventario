"""
Script Python para inventario completo: inventory_scheduled_queries.py

Este script genera un CSV con:

Nombre de la scheduled query.
ID/config resource.
Región.
Dataset destino.
Schedule.
Si está disabled.
Última ejecución.
Último estado.
Estatus calculado: activa/inactiva.
Query completo.
Tablas detectadas en FROM.
Tablas detectadas en JOIN.
Tablas detectadas en INSERT INTO.
Tablas wildcard.
Todas las tablas detectadas.
"""

import argparse
import csv
import json
import re
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery_datatransfer_v1 as bqdt


TABLE_REF_PATTERN = re.compile(
    r"""
    (?ix)
    \b(?P<clause>FROM|JOIN|INSERT\s+INTO)\s+
    (
        `(?P<backticked>[^`]+)`
        |
        (?P<plain>
            [A-Za-z_][A-Za-z0-9_-]*
            \.
            [A-Za-z_][A-Za-z0-9_-]*
            (?:
                \.
                [A-Za-z_][A-Za-z0-9_\-\$\*@]*
            )?
        )
    )
    """,
    re.VERBOSE | re.IGNORECASE,
)


def normalize_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    elif hasattr(value, "ToDatetime"):
        dt = value.ToDatetime()
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_param(config, key, default=""):
    try:
        value = config.params.get(key, default)
        return value if value is not None else default
    except Exception:
        return default


def state_to_str(state):
    try:
        return bqdt.TransferState(state).name
    except Exception:
        return str(state)


def clean_sql_comments(sql):
    if not sql:
        return ""

    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    return sql


def extract_tables(sql):
    """
    Extrae referencias de tablas en:
    - FROM dataset.table
    - FROM project.dataset.table
    - JOIN dataset.table
    - JOIN project.dataset.table
    - INSERT INTO dataset.table
    - INSERT INTO project.dataset.table
    - Con o sin backticks
    - Con wildcard tipo dataset.table_* o project.dataset.table_*
    """
    sql_clean = clean_sql_comments(sql)

    result = {
        "from_tables": [],
        "join_tables": [],
        "insert_into_tables": [],
        "wildcard_tables": [],
        "all_tables": [],
    }

    for match in TABLE_REF_PATTERN.finditer(sql_clean):
        clause = match.group("clause").upper().replace(" ", "_")
        table_ref = match.group("backticked") or match.group("plain")

        if not table_ref:
            continue

        table_ref = table_ref.strip()

        # Evita falsos positivos básicos.
        # Solo se aceptan dataset.table o project.dataset.table.
        parts = table_ref.split(".")
        if len(parts) not in (2, 3):
            continue

        if clause == "FROM":
            result["from_tables"].append(table_ref)
        elif clause == "JOIN":
            result["join_tables"].append(table_ref)
        elif clause == "INSERT_INTO":
            result["insert_into_tables"].append(table_ref)

        if "*" in table_ref:
            result["wildcard_tables"].append(table_ref)

        result["all_tables"].append(table_ref)

    for key in result:
        result[key] = sorted(set(result[key]))

    return result


def get_last_run_info(client, config_name):
    request = bqdt.ListTransferRunsRequest(
        parent=config_name,
        page_size=1000,
    )

    runs = list(client.list_transfer_runs(request=request))

    if not runs:
        return {
            "last_run_time": None,
            "last_run_state": None,
            "last_success_time": None,
            "runs_count": 0,
        }

    enriched_runs = []

    for run in runs:
        actual_time = (
            normalize_datetime(run.end_time)
            or normalize_datetime(run.start_time)
            or normalize_datetime(run.run_time)
            or normalize_datetime(run.schedule_time)
        )

        enriched_runs.append(
            {
                "time": actual_time,
                "state": state_to_str(run.state),
                "run": run,
            }
        )

    enriched_runs = [r for r in enriched_runs if r["time"] is not None]
    enriched_runs.sort(key=lambda x: x["time"], reverse=True)

    last_run = enriched_runs[0] if enriched_runs else None

    successful_runs = [
        r for r in enriched_runs
        if r["state"] in ("SUCCEEDED", "TransferState.SUCCEEDED")
    ]

    last_success = successful_runs[0] if successful_runs else None

    return {
        "last_run_time": last_run["time"] if last_run else None,
        "last_run_state": last_run["state"] if last_run else None,
        "last_success_time": last_success["time"] if last_success else None,
        "runs_count": len(runs),
    }


def classify_status(disabled, last_run_time, now_utc):
    if disabled:
        return "INACTIVA_DISABLED"

    if last_run_time is None:
        return "INACTIVA_SIN_EJECUCIONES"

    if last_run_time >= now_utc - timedelta(days=7):
        return "ACTIVA"

    return "INACTIVA_MAS_7_DIAS"


def build_inventory(project_id, locations):
    client = bqdt.DataTransferServiceClient()
    now_utc = datetime.now(timezone.utc)

    rows = []

    for location in locations:
        parent = f"projects/{project_id}/locations/{location}"

        request = bqdt.ListTransferConfigsRequest(
            parent=parent,
            data_source_ids=["scheduled_query"],
            page_size=1000,
        )

        configs = client.list_transfer_configs(request=request)

        for config in configs:
            if config.data_source_id != "scheduled_query":
                continue

            query = get_param(config, "query", "")
            extracted = extract_tables(query)

            run_info = get_last_run_info(client, config.name)

            last_run_time = run_info["last_run_time"]
            last_success_time = run_info["last_success_time"]

            status = classify_status(
                disabled=config.disabled,
                last_run_time=last_run_time,
                now_utc=now_utc,
            )

            row = {
                "project_id": project_id,
                "location": location,
                "config_name": config.name,
                "display_name": config.display_name,
                "data_source_id": config.data_source_id,
                "destination_dataset_id": config.destination_dataset_id,
                "schedule": config.schedule,
                "disabled": config.disabled,
                "status_migration": status,
                "last_run_time_utc": last_run_time.isoformat() if last_run_time else "",
                "last_run_state": run_info["last_run_state"] or "",
                "last_success_time_utc": last_success_time.isoformat() if last_success_time else "",
                "runs_count": run_info["runs_count"],
                "from_tables": json.dumps(extracted["from_tables"], ensure_ascii=False),
                "join_tables": json.dumps(extracted["join_tables"], ensure_ascii=False),
                "insert_into_tables": json.dumps(extracted["insert_into_tables"], ensure_ascii=False),
                "wildcard_tables": json.dumps(extracted["wildcard_tables"], ensure_ascii=False),
                "all_tables": json.dumps(extracted["all_tables"], ensure_ascii=False),
                "query": query,
            }

            rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument(
        "--locations",
        required=True,
        help="Lista separada por comas. Ejemplo: us,eu,us-central1",
    )
    parser.add_argument(
        "--output",
        default="scheduled_queries_inventory.csv",
    )

    args = parser.parse_args()

    locations = [x.strip() for x in args.locations.split(",") if x.strip()]
    rows = build_inventory(args.project_id, locations)

    fieldnames = [
        "project_id",
        "location",
        "config_name",
        "display_name",
        "data_source_id",
        "destination_dataset_id",
        "schedule",
        "disabled",
        "status_migration",
        "last_run_time_utc",
        "last_run_state",
        "last_success_time_utc",
        "runs_count",
        "from_tables",
        "join_tables",
        "insert_into_tables",
        "wildcard_tables",
        "all_tables",
        "query",
    ]

    with open(args.output, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Archivo generado: {args.output}")
    print(f"Scheduled queries encontradas: {len(rows)}")


if __name__ == "__main__":
    main()