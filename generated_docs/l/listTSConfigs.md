# listTSConfigs

## Location
[src/bin/psql/describe.c:5524-5572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5524-L5572)

## Overview
Lists PostgreSQL text search configurations with basic information, delegating to listTSConfigsVerbose for detailed output when verbose mode is requested.

## Definition
bool listTSConfigs(const char *pattern, bool verbose)

## Detailed Description
This function implements the \dF psql command for listing text search configurations from the pg_ts_config catalog. In non-verbose mode, it queries basic configuration information including schema, name, and description. When verbose mode is enabled, it delegates to listTSConfigsVerbose function for comprehensive configuration details including parser and dictionary mappings. The function supports pattern matching for selective configuration listing and uses PostgreSQL's visibility rules to show only accessible configurations.

## Parameters / Member Variables
- `pattern`: Pattern string for filtering configurations by name; if NULL, lists all visible configurations
- `verbose`: Boolean flag that determines output detail level; when true, delegates to listTSConfigsVerbose

## Dependencies
- Functions called/Symbols referenced:
  - [listTSConfigsVerbose](listTSConfigsVerbose.md) (when verbose=true)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command processor)

## Notes and Other Information
- Returns false on error, true on success
- Uses pg_ts_config_is_visible function to respect PostgreSQL's visibility rules
- Serves as a dispatcher function, handling simple listings internally and complex ones via delegation
- Text search configurations define how documents are parsed and indexed for full-text search
- Part of psql's text search object inspection functionality
- Results are ordered by schema name, then configuration name
- Implements internationalization through gettext_noop for column headers
- Provides a clean separation between simple and verbose configuration listing

## Simplified Source

```c
bool listTSConfigs(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Delegate to verbose function if verbose mode requested
    if (verbose) {
        return listTSConfigsVerbose(pattern);
    }

    // Initialize query buffer
    initPQExpBuffer(&buf);

    // Build SELECT query for basic configuration information
    printfPQExpBuffer(&buf,
        "SELECT n.nspname AS \"Schema\", "
        "c.cfgname AS \"Name\", "
        "pg_catalog.obj_description(c.oid, 'pg_ts_config') AS \"Description\" "
        "FROM pg_catalog.pg_ts_config c "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.cfgnamespace");

    // Validate and add pattern matching
    if (!validateSQLNamePattern(&buf, pattern, false, false,
                               "n.nspname", "c.cfgname", NULL,
                               "pg_catalog.pg_ts_config_is_visible(c.oid)",
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

    // Configure and display results
    myopt.title = "List of text search configurations";
    myopt.translate_header = true;
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

    PQclear(res);
    return true;
}
```