# applyLockingClause

## Location
src/backend/parser/analyze.c: 3529 - 3588

## Overview
Records locking information for a single range table entry by creating or updating a RowMarkClause in the query structure.

## Definition
```c
void applyLockingClause(Query *qry, Index rtindex, LockClauseStrength strength, LockWaitPolicy waitPolicy, bool pushedDown)
```

## Detailed Description
This function manages the application of row-level locking requirements to specific relations within a query. It handles the creation and updating of RowMarkClause structures that track which relations need locking and with what parameters.

The function implements precedence rules when multiple locking clauses apply to the same relation: stronger locking strengths override weaker ones, and more restrictive wait policies (NOWAIT > SKIP LOCKED > default waiting) take precedence. If a RowMarkClause already exists for the specified rtindex, the function updates it with the stronger settings rather than creating a duplicate.

The pushedDown parameter tracks whether the locking requirement originated from the current query level or was inherited from a parent query, which affects the hasForUpdate flag that influences query planning decisions.

## Parameters / Member Variables
- `qry`: The Query structure to modify with locking information
- `rtindex`: The range table index (1-based) of the relation to apply locking to
- `strength`: The LockClauseStrength enum value specifying the type of lock (FOR SHARE, FOR UPDATE, etc.)
- `waitPolicy`: The LockWaitPolicy enum value specifying behavior when encountering locked rows (wait, NOWAIT, SKIP LOCKED)
- `pushedDown`: Boolean indicating whether this locking clause was pushed down from a parent query level

## Dependencies
- Functions called/Symbols referenced:
  - get_parse_rowmark (checks for existing RowMarkClause)
  - makeNode (creates new RowMarkClause structures)
  - lappend (adds new clause to rowMarks list)
  - Max (macro for selecting maximum values)
  - Assert (assertion macro)
  - LCS_NONE (enum value for validation)
- Called from (representative examples):
  - transformLockingClause (multiple times during locking clause transformation)
  - markQueryForLocking (in rewrite handler for view rewriting)

## Notes and Other Information
- Exported function accessible from other modules
- Sets hasForUpdate flag on the query when explicit (non-pushed-down) locking clauses are applied
- Implements intelligent merging of multiple locking requirements for the same relation using precedence rules
- Stronger lock strengths override weaker ones (e.g., FOR UPDATE overrides FOR SHARE)
- More restrictive wait policies take precedence (NOWAIT > SKIP LOCKED > default waiting)
- pushedDown flag becomes false if any clause targeting the relation is explicit
- Creates RowMarkClause structures that are later used by the planner and executor
- Located in src/backend/parser/analyze.c at lines 3529-3588