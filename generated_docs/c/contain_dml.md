# contain_dml

## Location
src/backend/optimizer/plan/subselect.c: 1056 - 1061

## Overview
Determines whether any subquery in a query tree contains Data Manipulation Language (DML) operations or locking clauses, rejecting anything beyond plain SELECT statements.

## Definition
```c
static bool contain_dml(Node *node)
```

## Detailed Description
This function serves as a wrapper that checks whether a query tree contains any non-SELECT operations. It specifically identifies:

1. **DML Operations**: INSERT, UPDATE, DELETE statements
2. **Locking Clauses**: SELECT FOR UPDATE, SELECT FOR SHARE

The function is used primarily in CTE (Common Table Expression) processing to determine whether a CTE can be safely inlined. CTEs containing DML operations or locking clauses cannot be inlined because their side-effects need to be preserved and executed in a specific order.

The actual traversal logic is delegated to the `contain_dml_walker` function, following PostgreSQL's common pattern of having a simple wrapper function that calls a tree walker.

## Parameters
- `node`: The query tree node to examine for DML operations

## Dependencies
- Functions called/Symbols referenced:
  - contain_dml_walker
- Called from (representative examples):
  - SS_process_ctes

## Notes and Other Information
- Used specifically in CTE inlining decisions to prevent inlining of CTEs with side-effects
- Follows PostgreSQL's standard pattern of wrapper function + walker function for tree traversal
- Returns true if any DML or locking operations are found, false if only plain SELECT operations exist
- Critical for maintaining proper execution semantics when optimizing query plans