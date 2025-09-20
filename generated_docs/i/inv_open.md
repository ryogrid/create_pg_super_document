# inv_open

## Location
[src/backend/storage/large_object/inv_api.c:253-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L253-L336)

## Overview
Opens an existing large object and returns a descriptor for subsequent operations, with proper permission checking and snapshot management.

## Definition

```c
LargeObjectDesc *
inv_open(Oid lobjId, int flags, MemoryContext mcxt)
```
## Detailed Description
The  function provides access to an existing large object in PostgreSQL. It creates and returns a  structure that serves as a handle for subsequent large object operations. The function performs comprehensive validation including existence checks, permission verification, and proper snapshot management for transaction isolation.

The function supports different access modes through flags and handles memory allocation in the specified memory context. For write operations, it uses an instantaneous snapshot, while read operations use the currently active snapshot. The caller is responsible for ensuring the memory context and any associated snapshots have appropriate lifetimes.

## Parameters / Member Variables
- : The OID of the large object to open
- : Access mode flags (INV_READ for read access, INV_WRITE for write access, or both)
- : Memory context in which to allocate the descriptor and subsidiary data

## Dependencies
- Functions called/Symbols referenced:
  - GetActiveSnapshot
  - [myLargeObjectExists](../m/myLargeObjectExists.md)  
  - [pg_largeobject_aclcheck_snapshot](../p/pg_largeobject_aclcheck_snapshot.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [GetUserId](../G/GetUserId.md)
- Called from (representative examples):
  - [be_lo_open](../b/be_lo_open.md)
  - [lo_import_internal](../l/lo_import_internal.md)
  - [be_lo_export](../b/be_lo_export.md)
  - [lo_get_fragment_internal](../l/lo_get_fragment_internal.md)
  - [be_lo_from_bytea](../b/be_lo_from_bytea.md)
  - [be_lo_put](../b/be_lo_put.md)

## Notes and Other Information
- Historically, INV_WRITE automatically grants read access as well
- The function performs ACL permission checks unless lo_compat_privileges is enabled
- For write operations, an instantaneous snapshot is used; for read-only operations, the active snapshot is used
- The returned descriptor's snapshot field contains the snapshot used for the operation
- The caller must ensure proper lifetime management of the memory context and snapshot
- The descriptor's subid field is initialized to InvalidSubTransactionId and should be set by the caller if needed