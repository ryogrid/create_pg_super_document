# transformLockingClause

## Location
[src/backend/parser/analyze.c:3302-3528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L3302-L3528)

## Overview
Transforms and validates FOR UPDATE/SHARE clauses during query analysis by converting relation names to relids and applying locking semantics to appropriate relations.

## Definition
```c
static void transformLockingClause(ParseState *pstate, Query *qry, LockingClause *lc, bool pushedDown)
```

## Detailed Description
This static function is the core implementation for processing row locking clauses in PostgreSQL. It performs two main operations: validation using CheckSelectLocking(), and transformation of the locking clause by replacing relation names with integer relation identifiers (relids).

The function handles two scenarios: when no specific relations are named (locks all applicable relations in the query), and when specific relations are named in the locking clause. For each applicable relation, it calls applyLockingClause() to record the locking requirement and updates permission requirements.

The function recursively processes subqueries to ensure locking clauses are properly propagated through the query tree. It also performs extensive validation to ensure locking clauses are not applied to incompatible relation types (joins, functions, VALUES clauses, etc.).

## Parameters / Member Variables
- `pstate`: Parser state containing context information for error reporting and name resolution
- `qry`: The Query structure being processed, which will be modified to include locking information  
- `lc`: The LockingClause structure from the parsed statement containing locking strength, wait policy, and target relations
- `pushedDown`: Boolean flag indicating whether this locking clause was pushed down from a parent query level

## Dependencies
- Functions called/Symbols referenced:
  - [CheckSelectLocking](../C/CheckSelectLocking.md) (validates locking compatibility)
  - [applyLockingClause](../a/applyLockingClause.md) (applies locking to specific relations)
  - [LCS_asString](../L/LCS_asString.md) (for error message formatting)
  - makeNode (creates new LockingClause nodes)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md) (retrieves permission info for relations)
  - ereport (error reporting)
  - Various RTE type constants (RTE_RELATION, RTE_SUBQUERY, etc.)
- Called from (representative examples):
  - [transformSelectStmt](transformSelectStmt.md) (for main SELECT statements)
  - [transformSetOperationStmt](transformSetOperationStmt.md) (for set operations)
  - [transformPLAssignStmt](transformPLAssignStmt.md) (for PL/pgSQL assignments)
  - [transformLockingClause](transformLockingClause.md) (recursively for subqueries)

## Notes and Other Information
- Static function not directly accessible outside analyze.c
- Recursively calls itself to handle subqueries with locking clauses
- Updates ACL_SELECT_FOR_UPDATE permission requirements for affected relations
- Creates an "allrels" clause for propagating locking to subqueries
- Uses inFromCl flag to exclude auto-added RTEs like NEW/OLD in rules
- Provides detailed error messages for unsupported locking targets with specific RTE type handling
- Cross-references with markQueryForLocking() in rewriteHandler.c and isLockedRefname() in parse_relation.c
- Located in src/backend/parser/analyze.c at lines 3302-3528