# GetTableInfo

## Location
[src/bin/pgbench/pgbench.c:5344-5452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L5344-L5452)

## Overview
Extracts pgbench table information from the database to determine the scaling factor and partitioning configuration by querying the existing pgbench tables.

## Definition
```c
static void GetTableInfo(PGconn *con, bool scale_given)
```

## Detailed Description
This function gathers essential information about the existing pgbench database setup by querying system tables. It performs two main operations:

1. **Scale Factor Detection**: Queries the pgbench_branches table to count the number of branches, which determines the scale factor used during database initialization. This value is stored in the global  variable.

2. **Partition Information Discovery**: Examines the pgbench_accounts table to determine if it is partitioned and, if so, what partitioning method is used and how many partitions exist. This information is stored in global variables  and .

The function handles various error conditions gracefully, including missing tables (suggesting initialization is needed) and older PostgreSQL versions that don't support partitioning. For partitioned tables, it supports both range and hash partitioning methods.

If a scale factor was provided via command line but differs from the database contents, the function issues a warning and uses the database value instead.

## Parameters / Member Variables
- `con`: PostgreSQL connection handle for database queries
- `scale_given`: Boolean indicating whether the user specified a scale factor via command line (used for warning generation)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (execute SQL queries)
  - [PQresultStatus](../P/PQresultStatus.md) (check query result status)
  - PGRES_TUPLES_OK (successful query result constant)
  - [PQresultErrorField](../P/PQresultErrorField.md) (extract error details)
  - PG_DIAG_SQLSTATE (SQL state error field)
  - ERRCODE_UNDEFINED_TABLE (undefined table error code)
  - pg_log_error (error logging)
  - pg_log_error_hint (error hint logging)
  - pg_log_warning (warning logging)
  - [PQdb](../P/PQdb.md) (get database name from connection)
  - [PQgetvalue](../P/PQgetvalue.md) (extract result values)
  - [PQntuples](../P/PQntuples.md) (get number of result rows)
  - [PQgetisnull](../P/PQgetisnull.md) (check for NULL values)
  - [PQclear](../P/PQclear.md) (free result memory)
  - atoi (convert string to integer)
  - PART_NONE, PART_RANGE, PART_HASH (partitioning method constants)
  - Assert (debugging assertion)
- Called from (representative examples):
  - [main](../m/main.md) (during pgbench setup phase)

## Notes and Other Information
- Sets global variables: , , and 
- Provides helpful error hints suggesting database initialization when tables are missing
- Handles older PostgreSQL versions gracefully by assuming no partitioning on query failure
- The partitioning query uses a complex join to find the first pgbench_accounts table in the search_path
- Supports detection of range ('r') and hash ('h') partitioning methods
- Issues warnings when user-provided scale factors are overridden by database contents
- Exits the program on critical errors (missing tables, invalid data)
- The scale factor represents the number of branches and is used to calculate other table sizes
- Located in src/bin/pgbench/pgbench.c:5344-5452

## Simplified Source

```c
static void GetTableInfo(PGconn *con, bool scale_given)
{
    PGresult *res;

    // Get scaling factor from pgbench_branches table count
    res = PQexec(con, "select count(*) from pgbench_branches");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        char *sqlState = PQresultErrorField(res, PG_DIAG_SQLSTATE);

        pg_log_error("could not count number of branches: %s", PQerrorMessage(con));

        if (sqlState && strcmp(sqlState, ERRCODE_UNDEFINED_TABLE) == 0)
            pg_log_error_hint("Perhaps you need to do initialization (\"pgbench -i\") in database \"%s\".", PQdb(con));

        exit(1);
    }

    scale = atoi(PQgetvalue(res, 0, 0));
    if (scale < 0)
        pg_fatal("invalid count(*) from pgbench_branches: \"%s\"", PQgetvalue(res, 0, 0));

    PQclear(res);

    // Warn if user-provided scale is overridden
    if (scale_given)
        pg_log_warning("scale option ignored, using count from pgbench_branches table (%d)", scale);

    // Query partition information for pgbench_accounts table
    res = PQexec(con,
        "select o.n, p.partstrat, pg_catalog.count(i.inhparent) "
        "from pg_catalog.pg_class as c "
        "join pg_catalog.pg_namespace as n on (n.oid = c.relnamespace) "
        "cross join lateral (select pg_catalog.array_position(pg_catalog.current_schemas(true), n.nspname)) as o(n) "
        "left join pg_catalog.pg_partitioned_table as p on (p.partrelid = c.oid) "
        "left join pg_catalog.pg_inherits as i on (c.oid = i.inhparent) "
        "where c.relname = 'pgbench_accounts' and o.n is not null "
        "group by 1, 2 "
        "order by 1 asc "
        "limit 1");

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        // Assume no partitioning for older PostgreSQL versions
        partition_method = PART_NONE;
        partitions = 0;
    }
    else if (PQntuples(res) == 0) {
        pg_log_error("no pgbench_accounts table found in \"search_path\"");
        pg_log_error_hint("Perhaps you need to do initialization (\"pgbench -i\") in database \"%s\".", PQdb(con));
        exit(1);
    }
    else {
        // Extract partition information
        if (PQgetisnull(res, 0, 1))
            partition_method = PART_NONE;
        else {
            char *ps = PQgetvalue(res, 0, 1);

            if (strcmp(ps, "r") == 0)
                partition_method = PART_RANGE;
            else if (strcmp(ps, "h") == 0)
                partition_method = PART_HASH;
            else
                pg_fatal("unexpected partition method: \"%s\"", ps);
        }

        partitions = atoi(PQgetvalue(res, 0, 2));
    }

    PQclear(res);
}
```