"""Warehouse merge and validation helpers for Airflow task callables."""

from __future__ import annotations

import logging
from contextlib import closing
from typing import Any

from psycopg2 import sql

from pipeline_runtime import current_airflow_log_fields, db_connect, format_log_fields, split_table_name

logger = logging.getLogger(__name__)


def stage_table_has_rows(table_name: str) -> bool:
    """Return whether a stage or target table exists and contains at least one row."""

    schema_name, relation_name = split_table_name(table_name)

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(
            """
            select exists (
                select 1
                from information_schema.tables
                where table_schema = %s
                  and table_name = %s
            )
            """,
            (schema_name, relation_name),
        )
        if not cur.fetchone()[0]:
            return False

        cur.execute(
            sql.SQL("select exists (select 1 from {}.{} limit 1)").format(
                sql.Identifier(schema_name),
                sql.Identifier(relation_name),
            )
        )
        return bool(cur.fetchone()[0])


def merge_stage_into_target(
    target_table: str,
    stage_table: str,
    insert_columns: list[str],
    conflict_columns: list[str],
    *,
    update_columns: list[str] | None = None,
    drop_stage: bool = True,
    allow_missing_stage: bool = False,
) -> None:
    """Merge a Spark-written stage table into its stable serving table."""

    target_schema, target_relation = split_table_name(target_table)
    stage_schema, stage_relation = split_table_name(stage_table)
    update_columns = update_columns or [column for column in insert_columns if column not in conflict_columns]

    if allow_missing_stage and not stage_table_has_rows(stage_table):
        logger.info(
            "Skipping merge because the stage table is missing or empty %s",
            format_log_fields(**current_airflow_log_fields(), stage_table=stage_table, target_table=target_table),
        )
        return

    insert_identifiers = [sql.Identifier(column) for column in insert_columns]
    conflict_identifiers = [sql.Identifier(column) for column in conflict_columns]
    update_assignments = [
        sql.SQL("{} = excluded.{}").format(sql.Identifier(column), sql.Identifier(column))
        for column in update_columns
    ]

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        merge_statement = sql.SQL(
            """
            insert into {}.{} ({})
            select {}
            from {}.{}
            on conflict ({})
            do update set {}
            """
        ).format(
            sql.Identifier(target_schema),
            sql.Identifier(target_relation),
            sql.SQL(", ").join(insert_identifiers),
            sql.SQL(", ").join(insert_identifiers),
            sql.Identifier(stage_schema),
            sql.Identifier(stage_relation),
            sql.SQL(", ").join(conflict_identifiers),
            sql.SQL(", ").join(update_assignments),
        )
        cur.execute(merge_statement)
        if drop_stage:
            cur.execute(
                sql.SQL("drop table if exists {}.{}").format(
                    sql.Identifier(stage_schema),
                    sql.Identifier(stage_relation),
                )
            )
        conn.commit()
    logger.info(
        "Merged stage into target %s",
        format_log_fields(
            **current_airflow_log_fields(),
            stage_table=stage_table,
            target_table=target_table,
            conflict_columns=",".join(conflict_columns),
        ),
    )


def validate_table_has_rows(
    table_name: str,
    *,
    min_rows: int = 1,
    where_clause: str | None = None,
    params: list[Any] | tuple[Any, ...] | None = None,
    description: str | None = None,
    allow_missing_table: bool = False,
    allow_empty_result: bool = False,
) -> None:
    """Validate that a table contains at least the expected number of rows."""

    schema_name, relation_name = split_table_name(table_name)
    if allow_missing_table and not stage_table_has_rows(table_name):
        logger.info(
            "Skipping row validation because the table is missing %s",
            format_log_fields(**current_airflow_log_fields(), table_name=table_name, description=description or table_name),
        )
        return
    query = sql.SQL("select count(*) from {}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    )
    if where_clause:
        query += sql.SQL(" where ") + sql.SQL(where_clause)

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(query, params or [])
        row_count = int(cur.fetchone()[0])

    logger.info(
        "Validated row count %s",
        format_log_fields(
            **current_airflow_log_fields(),
            target_table=table_name,
            row_count=row_count,
            min_rows=min_rows,
            description=description or table_name,
        ),
    )
    if allow_empty_result and row_count == 0:
        logger.info(
            "Allowing empty row validation result %s",
            format_log_fields(**current_airflow_log_fields(), target_table=table_name, description=description or table_name),
        )
        return
    if row_count < min_rows:
        label = description or table_name
        raise ValueError(f"Validation failed: expected at least {min_rows} row(s) for {label}, found {row_count}")


def validate_distinct_values(
    table_name: str,
    column_name: str,
    *,
    min_distinct: int = 1,
    where_clause: str | None = None,
    params: list[Any] | tuple[Any, ...] | None = None,
    description: str | None = None,
    allow_empty_result: bool = False,
) -> None:
    """Validate that a table contains enough distinct values for one column."""

    schema_name, relation_name = split_table_name(table_name)
    query = sql.SQL("select count(distinct {}) from {}.{}").format(
        sql.Identifier(column_name),
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    )
    if where_clause:
        query += sql.SQL(" where ") + sql.SQL(where_clause)

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        cur.execute(query, params or [])
        distinct_count = int(cur.fetchone()[0])

    logger.info(
        "Validated distinct count %s",
        format_log_fields(
            **current_airflow_log_fields(),
            target_table=table_name,
            distinct_count=distinct_count,
            min_value=min_distinct,
            description=description or f"{table_name}.{column_name}",
        ),
    )
    if allow_empty_result and distinct_count == 0:
        logger.info(
            "Allowing empty distinct validation result %s",
            format_log_fields(**current_airflow_log_fields(), target_table=table_name, column_name=column_name),
        )
        return
    if distinct_count < min_distinct:
        label = description or f"{table_name}.{column_name}"
        raise ValueError(
            f"Validation failed: expected at least {min_distinct} distinct value(s) for {label}, found {distinct_count}"
        )


def validate_numeric_bounds(
    table_name: str,
    column_name: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    where_clause: str | None = None,
    params: list[Any] | tuple[Any, ...] | None = None,
    description: str | None = None,
    allow_empty_result: bool = False,
) -> None:
    """Validate that numeric values stay inside the allowed bounds."""

    schema_name, relation_name = split_table_name(table_name)
    conditions = [sql.SQL("{} is not null").format(sql.Identifier(column_name))]
    if min_value is not None:
        conditions.append(sql.SQL("{} < %s").format(sql.Identifier(column_name)))
    if max_value is not None:
        conditions.append(sql.SQL("{} > %s").format(sql.Identifier(column_name)))

    invalid_filter = conditions[0]
    if len(conditions) > 1:
        invalid_filter = sql.SQL("{} and ({})").format(
            conditions[0],
            sql.SQL(" or ").join(conditions[1:]),
        )

    query = sql.SQL("select count(*) from {}.{} where ").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    ) + invalid_filter

    query_params: list[Any] = []
    if min_value is not None:
        query_params.append(min_value)
    if max_value is not None:
        query_params.append(max_value)
    if where_clause:
        query += sql.SQL(" and (") + sql.SQL(where_clause) + sql.SQL(")")
        query_params.extend(list(params or []))

    with closing(db_connect()) as conn, closing(conn.cursor()) as cur:
        total_query = sql.SQL("select count(*) from {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
        )
        total_params: list[Any] = []
        if where_clause:
            total_query += sql.SQL(" where ") + sql.SQL(where_clause)
            total_params.extend(list(params or []))
        cur.execute(total_query, total_params)
        total_count = int(cur.fetchone()[0])
        cur.execute(query, query_params)
        invalid_count = int(cur.fetchone()[0])

    logger.info(
        "Validated numeric bounds %s",
        format_log_fields(
            **current_airflow_log_fields(),
            target_table=table_name,
            column_name=column_name,
            row_count=total_count,
            invalid_count=invalid_count,
            min_value=min_value,
            max_value=max_value,
            description=description or f"{table_name}.{column_name}",
        ),
    )
    if allow_empty_result and total_count == 0:
        logger.info(
            "Allowing empty numeric bounds validation result %s",
            format_log_fields(**current_airflow_log_fields(), target_table=table_name, column_name=column_name),
        )
        return
    if invalid_count > 0:
        bounds: list[str] = []
        if min_value is not None:
            bounds.append(f">= {min_value}")
        if max_value is not None:
            bounds.append(f"<= {max_value}")
        label = description or f"{table_name}.{column_name}"
        raise ValueError(f"Validation failed: {label} contains {invalid_count} row(s) outside bounds {' and '.join(bounds)}")
