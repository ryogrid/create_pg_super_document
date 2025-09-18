# PrintQueryStatus

## Location
src/bin/psql/common.c: 957 - 1003

## Overview
Prints the command status message for completed SQL queries, handling different output formats and logging while also setting the LASTOID psql variable.

## Definition
static void PrintQueryStatus(PGresult *result, FILE *printQueryFout)

## Detailed Description
This function handles the display of command status messages that PostgreSQL returns after executing SQL statements (e.g., "INSERT 0 1", "UPDATE 5", "DELETE 3"). It includes intelligent filtering to avoid printing status messages for SELECT queries unless they are from RETURNING clauses in INSERT/UPDATE/DELETE/MERGE statements. The function supports multiple output formats including HTML escaping, respects the quiet mode setting, and maintains a log file if configured. Additionally, it extracts and stores the OID of the last inserted row in the LASTOID psql variable.

Key features:
- Filters out status messages for plain SELECT queries
- Supports HTML output format with proper escaping
- Respects quiet mode (pset.quiet) to suppress output
- Logs status messages to logfile if configured
- Updates LASTOID variable with the OID from INSERT operations
- Uses the specified output file or falls back to default query output

## Parameters / Member Variables
- result: PGresult pointer containing the completed query result with status information
- printQueryFout: FILE pointer specifying the output destination, or NULL to use default (pset.queryFout)

## Dependencies
- Functions called/Symbols referenced:
  - [PQresultStatus](PQresultStatus.md) (gets the result status type)
  - [PQcmdStatus](PQcmdStatus.md) (retrieves the command status string)
  - [PQoidValue](PQoidValue.md) (extracts OID value from result)
  - strncmp (compares command prefixes)
  - [html_escaped_print](../h/html_escaped_print.md) (escapes HTML special characters)
  - fprintf/fputs (output functions)
  - fflush (flushes output stream)
  - snprintf (formats OID string)
  - SetVariable (sets psql variable)
- Constants referenced:
  - PGRES_TUPLES_OK (result status for SELECT-like queries)
  - PRINT_HTML (HTML output format constant)
- Global variables accessed:
  - pset.queryFout (default query output stream)
  - pset.quiet (quiet mode flag)
  - pset.popt.topt.format (output format setting)
  - pset.logfile (log file stream)
  - pset.vars (psql variables collection)
- Called from:
  - [PrintQueryResult](PrintQueryResult.md) (in src/bin/psql/common.c:1033, 1039)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (in src/bin/psql/common.c:1724)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function uses a local buffer for OID formatting with proper size limits
- HTML output includes proper paragraph tags and escaping for web display
- The LASTOID variable is always set regardless of output mode or quiet setting
- Status filtering logic specifically targets INSERT/UPDATE/DELETE/MERGE RETURNING clauses
- Output is always flushed to ensure immediate visibility
- The function handles NULL printQueryFout by falling back to the default output stream