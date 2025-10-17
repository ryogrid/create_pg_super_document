# compile_database_list

## Location
[src/bin/pg_amcheck/pg_amcheck.c:1583-1774](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_amcheck/pg_amcheck.c#L1583-L1774)

## Overview
Compiles a distinct list of PostgreSQL databases to check based on user-specified patterns and command-line options in the pg_amcheck utility.

## Definition

```c
static void
compile_database_list(PGconn *conn, SimplePtrList *databases,
					  const char *initial_dbname)
```
## Detailed Description
This function constructs a comprehensive list of databases to be checked by pg_amcheck. It handles various scenarios: explicit database patterns provided by the user, the --all flag for checking all databases, inclusion/exclusion pattern matching, and ensures proper filtering of connectable databases. The function uses a complex SQL query with multiple CTEs (Common Table Expressions) to efficiently resolve patterns against the pg_database catalog, applying inclusion and exclusion rules while respecting database connectivity constraints.

## Parameters / Member Variables  
- `conn`: PostgreSQL connection handle to the initial database for executing pattern resolution queries
- `databases`: Pointer to SimplePtrList structure that will be populated with DatabaseInfo objects representing databases to check
- `initial_dbname`: Optional initial database name to unconditionally include in the list (typically the connection database)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc0](../p/pg_malloc0.md)
  - pg_log_info  
  - [pstrdup](../p/pstrdup.md)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [append_db_pattern_cte](../a/append_db_pattern_cte.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [executeQuery](../e/executeQuery.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - pg_log_error
  - pg_log_error_detail
  - [disconnectDatabase](../d/disconnectDatabase.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - log_no_match
  - [PQclear](../P/PQclear.md)
  - [DatabaseInfo](../D/DatabaseInfo.md)
  - [SimplePtrList](../S/SimplePtrList.md)
  - [PQExpBufferData](../P/PQExpBufferData.md)
- Called from (representative examples):
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:499)
  - [main](../m/main.md) (at src/bin/pg_amcheck/pg_amcheck.c:513)

## Notes and Other Information
- Constructs a complex SQL query using multiple CTEs: include_raw, exclude_raw, database, include_pat, and filtered_databases
- Handles edge case where no database patterns exist and --all is not specified, avoiding unnecessary database queries
- Filters out non-connectable databases (datallowconn=false or datconnlimit=-2)
- Supports strict name checking mode where unmatched patterns cause fatal errors
- Prevents duplicate entries when initial_dbname matches a pattern-resolved database
- The generated SQL query efficiently combines inclusion/exclusion logic with database connectivity filtering
- Critical component of pg_amcheck's database discovery mechanism for pattern-based database selection

## Simplified Source

```c
static void compile_database_list(PGconn *conn, SimplePtrList *databases,
                                  const char *initial_dbname) {
    // Add initial database if specified
    if (initial_dbname) {
        DatabaseInfo *dat = (DatabaseInfo *) pg_malloc0(sizeof(DatabaseInfo));
        if (opts.verbose)
            pg_log_info("including database \"%s\"", initial_dbname);
        dat->datname = pstrdup(initial_dbname);
        simple_ptr_list_append(databases, dat);
    }

    PQExpBufferData sql;
    initPQExpBuffer(&sql);

    // Build CTE query with inclusion patterns
    appendPQExpBufferStr(&sql, "WITH include_raw (pattern_id, rgx) AS (");
    if (!append_db_pattern_cte(&sql, &opts.include, conn, true) && !opts.alldb) {
        // No patterns and not --all, skip database query
        termPQExpBuffer(&sql);
        return;
    }

    // Add exclusion patterns CTE
    appendPQExpBufferStr(&sql, "),\nexclude_raw (pattern_id, rgx) AS (");
    append_db_pattern_cte(&sql, &opts.exclude, conn, false);
    appendPQExpBufferStr(&sql, "),");

    // Build main query with CTEs for database filtering
    appendPQExpBufferStr(&sql,
        "\ndatabase (datname) AS ("
        "\nSELECT d.datname "
        "FROM pg_catalog.pg_database d "
        "LEFT OUTER JOIN exclude_raw e ON d.datname ~ e.rgx "
        "\nWHERE d.datallowconn AND datconnlimit != -2 "
        "AND e.pattern_id IS NULL"
        "),");

    // Add inclusion pattern checking and database selection logic
    appendPQExpBufferStr(&sql,
        "\ninclude_pat (pattern_id, checkable) AS ("
        "\nSELECT i.pattern_id, COUNT(*) FILTER (WHERE d IS NOT NULL) AS checkable"
        "\nFROM include_raw i LEFT OUTER JOIN database d ON d.datname ~ i.rgx"
        "\nGROUP BY i.pattern_id"
        "),");

    // Filter databases based on patterns or --all flag
    appendPQExpBufferStr(&sql, "\nfiltered_databases (datname) AS ("
        "\nSELECT DISTINCT d.datname FROM database d");
    if (!opts.alldb)
        appendPQExpBufferStr(&sql, " INNER JOIN include_raw i ON d.datname ~ i.rgx");

    // Final SELECT combining unmatched patterns and valid databases
    appendPQExpBufferStr(&sql, ")"
        "\nSELECT pattern_id, datname FROM ("
        "\nSELECT pattern_id, NULL::TEXT AS datname FROM include_pat WHERE checkable = 0 "
        "UNION ALL"
        "\nSELECT NULL, datname FROM filtered_databases"
        ") AS combined_records"
        "\nORDER BY pattern_id NULLS LAST, datname");

    // Execute query and process results
    PGresult *res = executeQuery(conn, sql.data, opts.echo);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log_error("query failed: %s", PQerrorMessage(conn));
        pg_log_error_detail("Query was: %s", sql.data);
        disconnectDatabase(conn);
        exit(1);
    }
    termPQExpBuffer(&sql);

    // Process query results
    bool fatal = false;
    int ntups = PQntuples(res);
    for (int i = 0; i < ntups; i++) {
        int pattern_id = -1;
        const char *datname = NULL;

        if (!PQgetisnull(res, i, 0))
            pattern_id = atoi(PQgetvalue(res, i, 0));
        if (!PQgetisnull(res, i, 1))
            datname = PQgetvalue(res, i, 1);

        if (pattern_id >= 0) {
            // Unmatched inclusion pattern
            fatal = opts.strict_names;
            log_no_match("no connectable databases to check matching \"%s\"",
                        opts.include.data[pattern_id].pattern);
        } else {
            // Valid database - add to list
            if (initial_dbname && strcmp(initial_dbname, datname) == 0)
                continue; // Skip duplicate

            if (opts.verbose)
                pg_log_info("including database \"%s\"", datname);

            DatabaseInfo *dat = (DatabaseInfo *) pg_malloc0(sizeof(DatabaseInfo));
            dat->datname = pstrdup(datname);
            simple_ptr_list_append(databases, dat);
        }
    }

    PQclear(res);
    if (fatal) {
        if (conn != NULL)
            disconnectDatabase(conn);
        exit(1);
    }
}
```