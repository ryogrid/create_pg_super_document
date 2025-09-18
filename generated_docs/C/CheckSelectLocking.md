# CheckSelectLocking

## Location
src/backend/parser/analyze.c: 3238 - 3301

## Overview
Validates that row locking clauses (FOR UPDATE/SHARE) are not used with incompatible SQL features in a query.

## Definition
```c
void CheckSelectLocking(Query *qry, LockClauseStrength strength)
```

## Detailed Description
This function performs semantic validation to ensure that row locking clauses are not combined with SQL features that are incompatible with row-level locking. PostgreSQL has several restrictions on where FOR UPDATE/SHARE clauses can be used, and this function enforces those restrictions by examining the query structure and raising appropriate errors.

The function checks for seven different incompatible features and generates specific error messages for each case using the LCS_asString helper to include the actual locking clause text in the error message. This validation occurs during query analysis and can also be called again by the planner after query rewriting and pullup operations.

## Parameters / Member Variables
- `qry`: Pointer to the Query structure being validated for locking compatibility  
- `strength`: The LockClauseStrength enum value indicating which type of row locking is being requested

## Dependencies
- Functions called/Symbols referenced:
  - [LCS_asString](../L/LCS_asString.md) (for error message formatting)
  - ereport (for error reporting)
  - Assert (assertion macro)
  - LCS_NONE (enum value)
- Called from (representative examples):
  - [transformLockingClause](../t/transformLockingClause.md) (during parse analysis)
  - [preprocess_rowmarks](../p/preprocess_rowmarks.md) (during query planning)

## Notes and Other Information
- Exported function that can be called from both parser and planner components
- Validates against these incompatible features:
  - Set operations (UNION/INTERSECT/EXCEPT)
  - DISTINCT clause
  - GROUP BY clause or grouping sets
  - HAVING clause  
  - Aggregate functions
  - Window functions
  - Set-returning functions in target list
- All error messages use ERRCODE_FEATURE_NOT_SUPPORTED error code
- The function assumes strength != LCS_NONE and will assert if called incorrectly
- Located in src/backend/parser/analyze.c at lines 3238-3301