# validateDomainCheckConstraint

## Location
src/backend/commands/typecmds.c: 3201 - 3320

## Overview
Validates that all existing data in columns of a specific domain type satisfy a new check constraint before it is officially added to the domain.

## Definition
```c
static void validateDomainCheckConstraint(Oid domainoid, const char *ccbin)
```

## Detailed Description
This function performs a comprehensive validation of all existing data that uses a specific domain type against a proposed check constraint. It locates all relations containing columns of the specified domain type, scans every tuple in those relations, and evaluates the constraint expression against each domain value. The function ensures data integrity by preventing the addition of constraints that would be violated by existing data.

The validation process uses the PostgreSQL executor infrastructure to evaluate the constraint expression in the context of actual domain values. If any existing value violates the constraint, the function immediately reports an error with detailed information about the violating column and table.

## Parameters / Member Variables
- `domainoid`: Object identifier of the domain type being validated
- `ccbin`: String representation of the check constraint expression in nodeToString format

## Dependencies
- Functions called/Symbols referenced:
  - stringToNode (parse constraint expression)
  - CreateExecutorState (create execution context)
  - GetPerTupleExprContext (get expression evaluation context)
  - ExecPrepareExpr (prepare expression for execution)
  - get_rels_with_domain (find relations using the domain)
  - table_beginscan/table_scan_getnextslot/table_endscan (table scanning)
  - ExecEvalExprSwitchContext (evaluate constraint expression)
  - ExecDropSingleTupleTableSlot (cleanup tuple slot)
  - FreeExecutorState (cleanup execution state)
- Called from:
  - AlterDomainAddConstraint (when adding new constraints)
  - AlterDomainValidateConstraint (when validating existing constraints)

## Notes and Other Information
- Uses ShareLock on relations to prevent concurrent data modifications during validation
- Holds relation locks until transaction commit, which may impact concurrency
- Provides detailed error messages including table and column names for constraint violations
- Part of the ALTER DOMAIN command implementation
- The function assumes the constraint expression is already properly formatted by nodeToString