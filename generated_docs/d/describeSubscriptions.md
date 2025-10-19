# describeSubscriptions

## Location
[src/bin/psql/describe.c:6525-6658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6525-L6658)

## Overview
Lists PostgreSQL logical replication subscriptions with their properties and configuration details, implementing the psql \dRs meta-command functionality.

## Definition

```c
bool
describeSubscriptions(const char *pattern, bool verbose)
```
## Detailed Description
The  function implements the  psql meta-command to display information about logical replication subscriptions. It provides both basic and verbose output modes, with the verbose mode showing additional configuration details that vary by PostgreSQL server version.

The function constructs a SQL query against the  system catalog and adapts the output columns based on:
1. The verbose flag parameter
2. PostgreSQL server version capabilities
3. Available subscription features in different versions

Key version-specific features displayed:
- PostgreSQL 10+: Basic subscription support (name, owner, enabled, publications)
- PostgreSQL 14+: Binary mode and streaming options
- PostgreSQL 15+: Two-phase commit, disable on error, skip LSN
- PostgreSQL 16+: Enhanced streaming modes (off/on/parallel), origin, password required, run as owner
- PostgreSQL 17+: Failover support

The function only shows subscriptions for the current database, filtering by .

## Parameters / Member Variables
- `*pattern`: Optional regular expression pattern to filter subscriptions by name. If NULL, all subscriptions in the current database are listed.
- `verbose`: Boolean flag controlling whether to show additional configuration details beyond the basic subscription information.
## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - lengthof
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRs command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (subscriptions were introduced in version 10)
- Only displays subscriptions for the current database (not cluster-wide)
- Uses dynamic column selection based on server version to avoid errors on older servers
- In verbose mode, shows extensive configuration details including:
  - Connection information (conninfo)
  - Synchronous commit settings
  - Binary transfer mode
  - Streaming configuration
  - Two-phase commit state
  - Error handling behavior
  - Security settings (password required, run as owner)
  - Failover capabilities
- Uses psql's standard query result formatting with internationalization support
- Returns boolean indicating success/failure of the operation
- Part of psql's describe.c module for \d commands

## Simplified Source

```c
bool describeSubscriptions(const char *pattern, bool verbose) {
    PQExpBufferData buf;
    PGresult *res;
    printQueryOpt myopt = pset.popt;

    // Check server version - subscriptions require PostgreSQL 10+
    if (pset.sversion < 100000) {
        pg_log_error("The server does not support subscriptions.");
        return true;
    }

    initPQExpBuffer(&buf);

    // Build base query with core subscription information
    printfPQExpBuffer(&buf,
        "SELECT subname AS \"Name\", "
        "pg_catalog.pg_get_userbyid(subowner) AS \"Owner\", "
        "subenabled AS \"Enabled\", "
        "subpublications AS \"Publication\"");

    // Add verbose columns based on server version
    if (verbose) {
        // Binary mode and streaming (PostgreSQL 14+)
        if (pset.sversion >= 140000) {
            appendPQExpBuffer(&buf, ", subbinary AS \"Binary\"");

            if (pset.sversion >= 160000) {
                // Enhanced streaming modes in PostgreSQL 16+
                appendPQExpBuffer(&buf,
                    ", (CASE substream "
                    "WHEN 'f' THEN 'off' "
                    "WHEN 't' THEN 'on' "
                    "WHEN 'p' THEN 'parallel' "
                    "END) AS \"Streaming\"");
            } else {
                appendPQExpBuffer(&buf, ", substream AS \"Streaming\"");
            }
        }

        // Two-phase and error handling (PostgreSQL 15+)
        if (pset.sversion >= 150000) {
            appendPQExpBuffer(&buf,
                ", subtwophasestate AS \"Two-phase commit\", "
                "subdisableonerr AS \"Disable on error\"");
        }

        // Origin and security settings (PostgreSQL 16+)
        if (pset.sversion >= 160000) {
            appendPQExpBuffer(&buf,
                ", suborigin AS \"Origin\", "
                "subpasswordrequired AS \"Password required\", "
                "subrunasowner AS \"Run as owner?\"");
        }

        // Failover support (PostgreSQL 17+)
        if (pset.sversion >= 170000) {
            appendPQExpBuffer(&buf, ", subfailover AS \"Failover\"");
        }

        // Connection and sync settings
        appendPQExpBuffer(&buf,
            ", subsynccommit AS \"Synchronous commit\", "
            "subconninfo AS \"Conninfo\"");

        // Skip LSN (PostgreSQL 15+)
        if (pset.sversion >= 150000) {
            appendPQExpBuffer(&buf, ", subskiplsn AS \"Skip LSN\"");
        }
    }

    // Filter to current database only
    appendPQExpBufferStr(&buf,
        " FROM pg_catalog.pg_subscription "
        "WHERE subdbid = (SELECT oid "
        "FROM pg_catalog.pg_database "
        "WHERE datname = pg_catalog.current_database())");

    // Apply pattern filter if provided
    if (!validateSQLNamePattern(&buf, pattern, true, false,
                                NULL, "subname", NULL, NULL, NULL, 1)) {
        termPQExpBuffer(&buf);
        return false;
    }

    appendPQExpBufferStr(&buf, " ORDER BY 1;");

    // Execute query
    res = PSQLexec(buf.data);
    termPQExpBuffer(&buf);
    if (!res)
        return false;

    // Set up output formatting
    myopt.title = "List of subscriptions";
    myopt.translate_header = true;

    // Display results and cleanup
    printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
    PQclear(res);

    return true;
}
```