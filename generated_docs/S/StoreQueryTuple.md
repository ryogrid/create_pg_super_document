# StoreQueryTuple

## Location
[src/bin/psql/common.c:762-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L762-L825)

## Overview
Stores the result of a query execution into psql variables as part of the \gset command functionality, extracting column values from a single-row result set and setting them as named variables.

## Definition

```c
static bool
StoreQueryTuple(const PGresult *result)
```
## Detailed Description
This function implements the core logic for the \gset psql meta-command, which allows users to store query results as psql variables. It validates that the result contains exactly one row, then iterates through all columns in that row, creating variables with names formed by concatenating the gset_prefix with each column name. NULL values result in unsetting the corresponding variable rather than setting it to an empty string.

The function performs several validation checks:
- Ensures exactly one row is returned (not zero, not multiple)
- Prevents overwriting specially treated variables that have hooks
- Handles NULL values by unsetting variables rather than setting them

## Parameters / Member Variables
- `*result`: A PGresult pointer containing the query execution results from libpq
## Dependencies
- Functions called/Symbols referenced:
  - [PQntuples](../P/PQntuples.md) (checks number of result rows)
  - [PQnfields](../P/PQnfields.md) (gets number of columns in result)
  - [PQfname](../P/PQfname.md) (retrieves column name)
  - [PQgetisnull](../P/PQgetisnull.md) (checks if column value is NULL)
  - [PQgetvalue](../P/PQgetvalue.md) (retrieves column value)
  - [VariableHasHook](../V/VariableHasHook.md) (checks if variable has special treatment)
  - [SetVariable](SetVariable.md) (sets psql variable)
  - pg_log_error (logs error messages)
  - pg_log_warning (logs warning messages)
  - [psprintf](../p/psprintf.md) (formatted string allocation)
- Called from:
  - [PrintQueryResult](../P/PrintQueryResult.md) (in src/bin/psql/common.c:1018)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function respects the global pset.gset_prefix setting for variable naming
- Memory management is handled properly with free() calls for allocated variable names
- Returns false on any error condition, allowing the caller to handle failures appropriately
- The \gset command is a psql-specific feature not part of standard SQL

## Simplified Source

```c
static bool StoreQueryTuple(const PGresult *result) {
    bool success = true;

    // Validate result has exactly one row
    if (PQntuples(result) < 1) {
        pg_log_error("no rows returned for \\gset");
        success = false;
    } else if (PQntuples(result) > 1) {
        pg_log_error("more than one row returned for \\gset");
        success = false;
    } else {
        // Process each column in the single row
        for (int i = 0; i < PQnfields(result); i++) {
            char *colname = PQfname(result, i);
            char *varname = psprintf("%s%s", pset.gset_prefix, colname);

            // Skip specially treated variables
            if (VariableHasHook(pset.vars, varname)) {
                pg_log_warning("attempt to \\gset into specially treated variable \"%s\" ignored", varname);
                continue;
            }

            // Set variable value or unset if NULL
            char *value = PQgetisnull(result, 0, i) ? NULL : PQgetvalue(result, 0, i);

            if (!SetVariable(pset.vars, varname, value)) {
                free(varname);
                success = false;
                break;
            }

            free(varname);
        }
    }

    return success;
}
```

This simplified version preserves the essential logic: validate single-row result, iterate through columns, create variables with prefix, handle NULL values by unsetting, and skip protected variables.