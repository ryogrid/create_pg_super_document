# RemoveOperatorById

## Location
[src/backend/commands/operatorcmds.c:413-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/operatorcmds.c#L413-L461)

## Overview
RemoveOperatorById performs the core deletion logic for removing an operator from the system catalog, handling commutator and negator link cleanup before removing the operator tuple.

## Definition
```c
void RemoveOperatorById(Oid operOid)
```

## Detailed Description
This function implements the low-level deletion logic for operator removal. It opens the pg_operator catalog table, retrieves the operator tuple, and handles the cleanup of bidirectional references before performing the actual deletion. The function ensures referential integrity by resetting commutator and negator links in related operators using OperatorUpd(), then removes the operator tuple from the catalog.

A key complexity handled by this function is self-referential operators (operators that are their own commutator or negator), which requires re-fetching the tuple after the OperatorUpd() call since the tuple may have been modified by the link updates.

The function operates at the catalog level and assumes all dependency checking and permission verification has already been performed by higher-level code.

## Parameters / Member Variables
- `operOid`: OID of the operator to be removed from the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close (catalog table access)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache (system cache operations) 
  - [OperatorUpd](../O/OperatorUpd.md) (commutator/negator link maintenance)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (tuple deletion from catalog)
  - Form_pg_operator (operator catalog structure)
  - HeapTupleIsValid, GETSTRUCT (tuple validation and access)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID conversion for cache lookup)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency system during DROP OPERATOR)

## Notes and Other Information
- This is the core deletion function called by the dependency system
- Handles cleanup of bidirectional commutator and negator relationships
- Requires RowExclusiveLock on the pg_operator catalog table
- Re-fetches the tuple if the operator is self-commutative or self-negating
- Assumes all dependency and permission checks have been performed by caller
- Does not perform cascade deletion - that is handled by the dependency system
- Uses system cache for efficient operator tuple lookup
- The OperatorUpd() call ensures related operators have their back-references cleared