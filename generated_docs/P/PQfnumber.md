# PQfnumber

## Location
src/interfaces/libpq/fe-exec.c: 3589 - 3685

## Overview
A public libpq function that finds the field (column) number for a given column name in a query result set, implementing SQL-style identifier parsing and case handling.

## Definition
```c
int PQfnumber(const PGresult *res, const char *field_name)
```

## Detailed Description
PQfnumber performs a reverse lookup to find the 0-based column number corresponding to a given column name. The function implements SQL identifier parsing rules, including case-folding (converting unquoted identifiers to lowercase) and double-quote processing for quoted identifiers. It includes an optimization path for all-lowercase field names that avoids string duplication and parsing overhead. The function handles both quoted and unquoted identifiers according to SQL standards, making it suitable for applications that need to map user-specified column names to their numeric indices.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing the query result data
- `field_name`: The column name to search for, which will be parsed according to SQL identifier rules

## Dependencies
- Functions called/Symbols referenced:
  - [pg_tolower](../p/pg_tolower.md) (for case-folding of unquoted characters)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- Returns the 0-based field number if found, -1 if not found or on error
- Returns -1 if res is NULL, field_name is NULL/empty, or res->attDescs is NULL
- Implements SQL identifier parsing: unquoted names are case-folded to lowercase, quoted names preserve case
- Handles escaped quotes within quoted identifiers ("" becomes ")
- Includes performance optimization for all-lowercase names to avoid string duplication
- May find the first match if multiple columns have the same name (though this is rare)
- Part of the public libpq API (declared in libpq-fe.h)
- Uses dynamic memory allocation (strdup/free) for complex identifier parsing
- Does not fully validate SQL identifier syntax (e.g., partially quoted strings are processed without error)