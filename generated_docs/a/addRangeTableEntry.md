# addRangeTableEntry

## Location
[src/backend/parser/parse_relation.c:1470-1566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L1470-L1566)

## Overview
Creates a range table entry for a relation and adds it to the parser state, returning a ParseNamespaceItem for use in query parsing and name resolution.

## Definition

```c
struct and return a ParseNamespaceItem for the new RTE.
 *
 * This is just like addRangeTableEntry() except that it makes an RTE
 * given an already-open relation instead of a RangeVar reference.
 *
 * lockmode is the lock type required for query execution;
```
## Detailed Description
The  function is a core parser utility that creates and initializes a RangeTblEntry (RTE) for a relation reference in a SQL query. It handles the complete process of:

1. Creating a new RTE with type RTE_RELATION
2. Determining the appropriate lock mode based on whether the relation is referenced in FOR UPDATE/SHARE clauses
3. Opening the relation with proper locking to validate existence and get metadata
4. Building effective column names using aliases or actual column names
5. Setting up permission information with default SELECT access
6. Adding the RTE to the parser state's range table
7. Creating and returning a ParseNamespaceItem for namespace resolution

The function ensures proper relation access control and maintains referential integrity during query parsing. It does not handle refname conflicts - that responsibility lies with the caller to check conflicts in the appropriate scope.

## Parameters / Member Variables
- : Parser state containing the range table and other parsing context
- : RangeVar specifying the relation name and schema information
- : Optional alias for the relation; if NULL, uses the relation's actual name
- : Boolean indicating whether inheritance should be considered for the relation
- : Boolean indicating whether this entry originates from a FROM clause

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RTE creation)
  - [isLockedRefname](../i/isLockedRefname.md) (lock mode determination)
  - [parserOpenTable](../p/parserOpenTable.md) (relation opening with locks)
  - RelationGetRelid (OID extraction)
  - [makeAlias](../m/makeAlias.md) (alias creation)
  - [buildRelationAliases](../b/buildRelationAliases.md) (column name building)
  - [addRTEPermissionInfo](addRTEPermissionInfo.md) (permission setup)
  - lappend (list manipulation)
  - [buildNSItemFromTupleDesc](../b/buildNSItemFromTupleDesc.md) (namespace item creation)
  - table_close (relation cleanup)
- Called from (representative examples):
  - [transformTableEntry](../t/transformTableEntry.md) (in parse_clause.c)

## Notes and Other Information
- The function maintains access locks until end of transaction to prevent schema modifications
- Default permission is ACL_SELECT; callers must modify for target tables requiring write access
- The returned ParseNamespaceItem is not automatically added to the parser state's namespace - caller must handle this appropriately
- Lock mode determination uses RowShareLock for relations in FOR UPDATE/SHARE, AccessShareLock otherwise
- This is typically the first access to a relation in a statement, establishing proper locking protocol