# listTSParsersVerbose

## Location
[src/bin/psql/describe.c:5199-5273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5199-L5273)

## Overview
Provides detailed information about text search parsers matching a given pattern by querying PostgreSQL's text search parser catalog and displaying verbose descriptions for each parser.

## Definition
static bool listTSParsersVerbose(const char *pattern)

## Detailed Description
This function implements the verbose listing functionality for PostgreSQL text search parsers in psql. It queries the pg_ts_parser catalog to retrieve parser information including OID, namespace, and parser name. For each matching parser, it calls describeOneTSParser to display detailed information about the parser's functions and token types. The function supports pattern matching for selective parser listing and provides appropriate error messages when no parsers are found.

## Parameters / Member Variables
- : Pattern string for filtering parsers by name; if NULL, lists all visible parsers

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [describeOneTSParser](../d/describeOneTSParser.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [listTSParsers](listTSParsers.md)

## Notes and Other Information
- Returns false on error or when no parsers match the pattern
- Handles cancellation through the cancel_pressed global variable
- Uses PostgreSQL's visibility rules via pg_ts_parser_is_visible function
- Provides user-friendly error messages when no parsers are found
- Part of psql's \dFp+ command implementation for verbose parser listing

## Simplified Source

```c
static bool listTSParsersVerbose(const char *pattern) {
    PQExpBufferData buf;
    PGresult *res;

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build query to get parser OID, namespace, and name
    printfPQExpBuffer(&buf,
        "SELECT p.oid, n.nspname, p.prsname "
        "FROM pg_catalog.pg_ts_parser p "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.prsnamespace");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "n.nspname", "p.prsname", NULL,
                               "pg_catalog.pg_ts_parser_is_visible(p.oid)",
                               NULL, 3)) {
        termPQExpBuffer(&buf);
        return false;
    }

    // Add ordering
    appendPQExpBufferStr(&buf, " ORDER BY 1, 2;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res) return false;

    // Check if any parsers found
    if (PQntuples(res) == 0) {
        if (!pset.quiet) {
            if (pattern) {
                pg_log_error("Did not find any text search parser named \"%s\".", pattern);
            } else {
                pg_log_error("Did not find any text search parsers.");
            }
        }
        PQclear(res);
        return false;
    }

    // Describe each parser in detail
    for (int i = 0; i < PQntuples(res); i++) {
        const char *oid = PQgetvalue(res, i, 0);
        const char *nspname = PQgetisnull(res, i, 1) ? NULL : PQgetvalue(res, i, 1);
        const char *prsname = PQgetvalue(res, i, 2);

        // Show detailed parser information
        if (!describeOneTSParser(oid, nspname, prsname)) {
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