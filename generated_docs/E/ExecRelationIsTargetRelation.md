# ExecRelationIsTargetRelation

## Location
[src/backend/executor/execUtils.c:684-696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L684-L696)

## Overview
Determines whether a relation (identified by range table index) is one of the target relations of the current query.

## Definition
```c
bool ExecRelationIsTargetRelation(EState *estate, Index scanrelid)
```

## Detailed Description
ExecRelationIsTargetRelation is a utility function that checks if a given relation, specified by its range table index, is among the target relations for the current query execution. Target relations are those that will be modified by the query (e.g., in INSERT, UPDATE, DELETE operations).

The function works by examining the resultRelations list in the planned statement within the execution state. It uses the PostgreSQL list utility function list_member_int to check if the provided scanrelid exists in this list of target relation indices.

Although this function is no longer actively used in PostgreSQL's core executor code, it is maintained for backward compatibility and potential use by Foreign Data Wrapper (FDW) implementations that may need to determine if their foreign table is a target of modification operations.

## Parameters / Member Variables
- `estate`: Execution state containing the planned statement and query execution context
- `scanrelid`: Index identifying the relation in the range table to check against target relations

## Dependencies
- Functions called/Symbols referenced:
  - list_member_int
- Called from (representative examples):
  - ResetPerTupleExprContext

## Notes and Other Information
- This function is no longer used in PostgreSQL core code but is preserved for FDW compatibility
- Provides a clean interface for FDWs to determine if their foreign table is a modification target
- The function performs a simple membership test on the resultRelations list
- Located in src/backend/executor/execUtils.c:684-696
- Returns true if the relation is a target relation, false otherwise
- Target relations are those specified in the resultRelations list of the planned statement