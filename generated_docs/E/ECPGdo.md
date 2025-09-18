# ECPGdo

## Location
src/interfaces/ecpg/ecpglib/execute.c: 2277 - 2291

## Overview
The public API function for executing SQL statements in the ECPG library that provides a variable-argument interface to the underlying execution engine.

## Definition
bool ECPGdo(const int lineno, const int compat, const int force_indicator, const char *connection_name, const bool questionmarks, const int st, const char *query, ...)

## Detailed Description
ECPGdo serves as the primary public interface for executing embedded SQL statements in PostgreSQL's ECPG library. It acts as a wrapper around the internal ecpg_do function, providing a convenient variable-argument interface that allows callers to pass parameters directly as function arguments rather than as a va_list. The function handles the conversion from variable arguments to va_list format and delegates the actual execution to ecpg_do, ensuring consistent behavior across all SQL execution paths in the ECPG library.

## Parameters / Member Variables
- lineno: Line number in the source code where the SQL statement appears (for error reporting)
- compat: Compatibility mode setting for ECPG behavior
- force_indicator: Flag to force indicator variable handling
- connection_name: Name of the database connection to use (NULL for default connection)
- questionmarks: Boolean flag indicating whether the query uses question mark parameter placeholders
- st: Statement type as an integer (cast to ECPG_statement_type enum)
- query: The SQL query string to execute
- ...: Variable arguments containing parameters for the query

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_do
- Called from (representative examples):
  - (This is typically called by ECPG-generated code)

## Notes and Other Information
- Returns true on successful execution, false on failure
- This is the main entry point for SQL execution in ECPG-generated code
- Uses standard C variable argument handling (va_start, va_end)
- Located in src/interfaces/ecpg/ecpglib/execute.c:2277-2291
- The function signature matches what the ECPG preprocessor generates for embedded SQL statements