# listTSConfigsVerbose

## Location
[src/bin/psql/describe.c:5573-5656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5573-L5656)

## Overview
Lists all text search configurations in the PostgreSQL database with detailed information, including their associated parsers and namespaces.

## Definition
static bool listTSConfigsVerbose(const char *pattern)

## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve comprehensive information about text search configurations. It performs a JOIN operation across multiple catalog tables (pg_ts_config, pg_namespace, pg_ts_parser) to gather configuration names, their namespaces, associated parsers, and parser namespaces. For each configuration found, it calls describeOneTSConfig to display detailed token-to-dictionary mappings. The function supports pattern matching to filter results and provides verbose output showing the internal structure of text search configurations.

## Parameters / Member Variables
- : Optional SQL pattern to filter text search configuration names (can be NULL to show all configurations)

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) 
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [describeOneTSConfig](../d/describeOneTSConfig.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [listTSConfigs](listTSConfigs.md)

## Notes and Other Information
- Returns false if no configurations match the pattern or if an error occurs
- Displays error messages when no configurations are found (unless pset.quiet is set)
- Supports cancellation via cancel_pressed global variable
- Orders results by namespace and configuration name for consistent output
- This is a static function used internally by psql's describe functionality for text search configurations

## Simplified Source

```c
static bool listTSConfigsVerbose(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build query to get configuration info with parser details
    printfPQExpBuffer(&buf,
        "SELECT c.oid, c.cfgname, n.nspname, p.prsname, np.nspname AS pnspname "
        "FROM pg_catalog.pg_ts_config c "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.cfgnamespace, "
        "pg_catalog.pg_ts_parser p "
        "LEFT JOIN pg_catalog.pg_namespace np ON np.oid = p.prsnamespace "
        "WHERE p.oid = c.cfgparser");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                               "n.nspname", "c.cfgname", NULL,
                               "pg_catalog.pg_ts_config_is_visible(c.oid)",
                               NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Add ordering
    appendPQExpBufferStr(&buf, " ORDER BY 3, 2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Check if any configurations found
    if (PQntuples(res) == 0) {
        if (!pset.quiet) {
            if (pattern) {
                pg_log_error("Did not find any text search configuration named \"%s\".", pattern);
            } else {
                pg_log_error("Did not find any text search configurations.");
            }
        }
        PQclear(res);
        return false;
    }

    // Describe each configuration in detail
    for (int i = 0; i < PQntuples(res); i++) {
        const char *oid = PQgetvalue(res, i, 0);
        const char *cfgname = PQgetvalue(res, i, 1);
        const char *nspname = PQgetisnull(res, i, 2) ? NULL : PQgetvalue(res, i, 2);
        const char *prsname = PQgetvalue(res, i, 3);
        const char *pnspname = PQgetisnull(res, i, 4) ? NULL : PQgetvalue(res, i, 4);

        // Show detailed configuration information
        if (!describeOneTSConfig(oid, nspname, cfgname, pnspname, prsname)) {
            PQclear(res);
            return false;
        }

        // Check for user cancellation
        if (cancel_pressed) {
            PQclear(res);
            return false;
        }
    }

    PQclear(res);
    return true;
}
```