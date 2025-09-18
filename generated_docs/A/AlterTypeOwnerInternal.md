# AlterTypeOwnerInternal

## Location
src/backend/commands/typecmds.c: 3987 - 4054

## Overview
Core implementation function that performs the actual pg_type catalog modifications for type ownership changes and recursively handles dependent array and multirange types.

## Definition
```c
void AlterTypeOwnerInternal(Oid typeOid, Oid newOwnerId)
```

## Detailed Description
AlterTypeOwnerInternal is the fundamental function that implements type ownership changes at the catalog level. It directly modifies the pg_type system catalog to update the typowner field and handles ACL (Access Control List) adjustments when necessary. The function implements a recursive strategy to automatically handle dependent types including array types and multirange types.

The function uses heap_modify_tuple to update the type tuple, ensuring atomic updates to both ownership and ACL information. For types with associated array types, it recursively calls itself to maintain consistency. Range types receive special handling where their associated multirange types are also updated recursively.

## Parameters / Member Variables
- `typeOid`: The OID of the type whose ownership is being changed
- `newOwnerId`: The OID of the role that will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCacheCopy1
  - heap_getattr
  - aclnewowner
  - DatumGetAclP
  - heap_modify_tuple
  - CatalogTupleUpdate
  - get_range_multirange
  - AlterTypeOwnerInternal (recursive calls)
  - table_close
  - TYPTYPE_RANGE
- Called from (representative examples):
  - AlterTypeOwner_oid
  - ATExecChangeOwner
  - AlterTypeOwnerInternal (recursive self-calls)

## Notes and Other Information
- This is a void function that operates directly on the system catalogs
- Uses RowExclusiveLock on TypeRelationId throughout the operation
- Handles ACL updates only when the type has a non-null ACL (typacl field)
- Implements automatic recursive handling of array types via typarray field
- Provides special recursive handling for range types and their associated multirange types
- Updates both typowner and typacl fields atomically using heap_modify_tuple
- Self-recursive design ensures all dependent types maintain ownership consistency
- Used as the lowest-level implementation by both table and type ownership change operations
- Error handling includes validation that multirange types exist for range types