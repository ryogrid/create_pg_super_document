# prepare_vacuum_command

## Location
[src/bin/scripts/vacuumdb.c:976-1145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L976-L1145)

## Overview
This function constructs a SQL VACUUM or ANALYZE command string based on provided options and server version compatibility. It builds version-aware commands that utilize appropriate syntax for different PostgreSQL server versions.

## Definition

```c
static void
prepare_vacuum_command(PQExpBuffer sql, int serverVersion,
					   vacuumingOptions *vacopts, const char *table)
```
## Detailed Description
The function generates SQL commands for database maintenance operations (VACUUM/ANALYZE) by examining the provided options and constructing syntax appropriate for the target PostgreSQL server version. It handles two main command types:

1. **ANALYZE-only commands**: When  is true, generates ANALYZE statements with optional parameters like SKIP_LOCKED, VERBOSE, and BUFFER_USAGE_LIMIT.

2. **VACUUM commands**: Constructs VACUUM statements with extensive option support including FULL, FREEZE, VERBOSE, ANALYZE, PARALLEL, INDEX_CLEANUP control, TRUNCATE control, and various process control options.

The function uses version checks to ensure compatibility, as different options were introduced in different PostgreSQL versions. It employs parenthesized syntax for newer versions (v9.0+ for VACUUM, v11+ for ANALYZE) and falls back to older syntax for compatibility.

## Parameters / Member Variables
- : PQExpBuffer to store the constructed SQL command string
- : Integer representing PostgreSQL server version (e.g., 120000 for v12.0)
- : Pointer to vacuumingOptions structure containing all vacuum/analyze options
- : Pre-quoted table name string to include in the command

### vacuumingOptions Structure Members:
- : If true, generates ANALYZE command instead of VACUUM
- : Enables verbose output in the command
- : Adds ANALYZE option to VACUUM command
- : Enables FULL vacuum mode
- : Enables FREEZE option
- : Disables page skipping optimization (v9.6+)
- : Skips locked tables/rows (v12+)
- : Number of parallel workers for VACUUM (v13+)
- : Disables index cleanup (v12+)
- : Forces index cleanup (v12+)
- : Controls table truncation (v12+)
- : Controls main table processing (v16+)
- : Controls TOAST table processing (v14+)
- : Skips database-wide statistics (v16+)
- : Sets buffer usage limit (v16+)

## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [vacuumingOptions](../v/vacuumingOptions.md) (structure)
- Called from:
  - [vacuum_one_database](../v/vacuum_one_database.md)

## Notes and Other Information
- The function is static and only used within vacuumdb.c
- Extensive version checking ensures backward compatibility with older PostgreSQL servers
- The table name parameter must be properly quoted before being passed to this function
- Generated commands are semicolon-terminated
- Uses Assert() statements to verify version requirements for newer options
- Handles mutual exclusivity of conflicting options (e.g., no_index_cleanup vs force_index_cleanup)

## Simplified Source

```c
static void prepare_vacuum_command(PQExpBuffer sql, int serverVersion,
                                   vacuumingOptions *vacopts, const char *table) {
    const char *paren = " (";
    const char *comma = ", ";
    const char *sep = paren;

    resetPQExpBuffer(sql);

    if (vacopts->analyze_only) {
        // Build ANALYZE command
        appendPQExpBufferStr(sql, "ANALYZE");

        // Add parenthesized options for v11+
        if (serverVersion >= 110000) {
            if (vacopts->skip_locked) {
                appendPQExpBuffer(sql, "%sSKIP_LOCKED", sep);
                sep = comma;
            }
            if (vacopts->verbose) {
                appendPQExpBuffer(sql, "%sVERBOSE", sep);
                sep = comma;
            }
            if (vacopts->buffer_usage_limit) {
                appendPQExpBuffer(sql, "%sBUFFER_USAGE_LIMIT '%s'", sep,
                                  vacopts->buffer_usage_limit);
                sep = comma;
            }
            if (sep != paren)
                appendPQExpBufferChar(sql, ')');
        } else {
            // Use old syntax for older versions
            if (vacopts->verbose)
                appendPQExpBufferStr(sql, " VERBOSE");
        }
    } else {
        // Build VACUUM command
        appendPQExpBufferStr(sql, "VACUUM");

        // Add parenthesized options for v9.0+
        if (serverVersion >= 90000) {
            if (vacopts->disable_page_skipping) {
                appendPQExpBuffer(sql, "%sDISABLE_PAGE_SKIPPING", sep);
                sep = comma;
            }
            if (vacopts->no_index_cleanup) {
                appendPQExpBuffer(sql, "%sINDEX_CLEANUP FALSE", sep);
                sep = comma;
            }
            if (vacopts->force_index_cleanup) {
                appendPQExpBuffer(sql, "%sINDEX_CLEANUP TRUE", sep);
                sep = comma;
            }
            if (!vacopts->do_truncate) {
                appendPQExpBuffer(sql, "%sTRUNCATE FALSE", sep);
                sep = comma;
            }
            if (!vacopts->process_main) {
                appendPQExpBuffer(sql, "%sPROCESS_MAIN FALSE", sep);
                sep = comma;
            }
            if (!vacopts->process_toast) {
                appendPQExpBuffer(sql, "%sPROCESS_TOAST FALSE", sep);
                sep = comma;
            }
            if (vacopts->skip_database_stats) {
                appendPQExpBuffer(sql, "%sSKIP_DATABASE_STATS", sep);
                sep = comma;
            }
            if (vacopts->skip_locked) {
                appendPQExpBuffer(sql, "%sSKIP_LOCKED", sep);
                sep = comma;
            }
            if (vacopts->full) {
                appendPQExpBuffer(sql, "%sFULL", sep);
                sep = comma;
            }
            if (vacopts->freeze) {
                appendPQExpBuffer(sql, "%sFREEZE", sep);
                sep = comma;
            }
            if (vacopts->verbose) {
                appendPQExpBuffer(sql, "%sVERBOSE", sep);
                sep = comma;
            }
            if (vacopts->and_analyze) {
                appendPQExpBuffer(sql, "%sANALYZE", sep);
                sep = comma;
            }
            if (vacopts->parallel_workers >= 0) {
                appendPQExpBuffer(sql, "%sPARALLEL %d", sep,
                                  vacopts->parallel_workers);
                sep = comma;
            }
            if (vacopts->buffer_usage_limit) {
                appendPQExpBuffer(sql, "%sBUFFER_USAGE_LIMIT '%s'", sep,
                                  vacopts->buffer_usage_limit);
                sep = comma;
            }
            if (sep != paren)
                appendPQExpBufferChar(sql, ')');
        } else {
            // Use old syntax for older versions
            if (vacopts->full)
                appendPQExpBufferStr(sql, " FULL");
            if (vacopts->freeze)
                appendPQExpBufferStr(sql, " FREEZE");
            if (vacopts->verbose)
                appendPQExpBufferStr(sql, " VERBOSE");
            if (vacopts->and_analyze)
                appendPQExpBufferStr(sql, " ANALYZE");
        }
    }

    // Add table name and terminate with semicolon
    appendPQExpBuffer(sql, " %s;", table);
}
```