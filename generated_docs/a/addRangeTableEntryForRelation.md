# addRangeTableEntryForRelation

## Location
[src/backend/parser/parse_relation.c:1567-1637](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1567-L1637)

## Overview
Creates a range table entry for an already-open relation and adds it to the parser state, returning a ParseNamespaceItem for use in query parsing and name resolution.

## Definition


## Detailed Description
The  function is a specialized version of  that works with an already-open relation instead of a RangeVar reference. This function is particularly useful when the caller has already opened the relation with appropriate locks and wants to create a range table entry without going through the relation lookup process again.

Key characteristics:
1. Accepts an already-open Relation instead of a RangeVar
2. Requires the caller to specify and hold the appropriate lock mode
3. Validates that the caller holds the required lock through assertions
4. Creates an RTE with type RTE_RELATION and initializes all necessary fields
5. Builds effective column names and sets up permission information
6. Returns a ParseNamespaceItem without adding it to the parser's namespace

The function includes strict assertions to ensure lock mode validity (must be AccessShareLock, RowShareLock, or RowExclusiveLock) and that the caller actually holds the specified lock mode.

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : Already-open relation structure with appropriate locks held
- : Lock type required for query execution (AccessShareLock, RowShareLock, or RowExclusiveLock)
- : Optional alias for the relation; if NULL, uses the relation's actual name  
- : Boolean indicating whether inheritance should be considered for the relation
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE creation)
  - RelationGetRelationName (relation name extraction)
  - [CheckRelationLockedByMe](../C/CheckRelationLockedByMe.md) (lock validation)
  - RelationGetRelid (OID extraction)
  - [makeAlias](../m/makeAlias.md) (alias creation)
  - [buildRelationAliases](../b/buildRelationAliases.md) (column name building)
  - [addRTEPermissionInfo](addRTEPermissionInfo.md) (permission setup)
  - lappend (list manipulation)
  - [buildNSItemFromTupleDesc](../b/buildNSItemFromTupleDesc.md) (namespace item creation)
- Called from (representative examples):
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md) (in heap.c)
  - [DoCopy](../D/DoCopy.md) (in copy.c)
  - [CreatePolicy](../C/CreatePolicy.md) (in policy.c)
  - [setTargetTable](../s/setTargetTable.md) (in parse_clause.c)
  - [transformOnConflictClause](../t/transformOnConflictClause.md) (in analyze.c)

## Notes and Other Information
- The caller must hold the specified lock mode or a stronger one before calling this function
- Lock mode parameter is declared as int rather than LOCKMODE to avoid header dependencies
- This function provides better performance when the relation is already open, avoiding redundant lookups
- Strict assertion checks ensure proper locking protocol is followed
- Default permission is ACL_SELECT; callers must modify for target tables requiring write access
- Used extensively in DDL operations, constraint processing, and rule rewriting where relations are already open