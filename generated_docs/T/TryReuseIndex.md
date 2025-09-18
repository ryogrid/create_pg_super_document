# TryReuseIndex

## Location
[src/backend/commands/tablecmds.c:14291-14318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14291-L14318)

## Overview
TryReuseIndex is a subroutine used during column type alteration to determine if an existing index can be reused with new column definitions, and if so, marks the IndexStmt with the storage information needed for reuse.

## Definition
```c
static void
TryReuseIndex(Oid oldId, IndexStmt *stmt)
```

## Detailed Description
This function is a helper routine for ATPostAlterTypeParse() that attempts to reuse an existing index during column type alteration operations. It leverages CheckIndexCompatible() to determine if the existing index with the given OID is compatible with the proposed new index specification. If compatibility is confirmed, the function extracts storage-related metadata from the existing index and stores it in the IndexStmt structure to enable physical storage reuse.

The function specifically handles partitioned indexes by checking the relation kind and only setting storage reuse parameters for non-partitioned indexes, since partitioned indexes have no physical storage to share.

## Parameters
- `oldId`: OID of the existing index to potentially reuse
- `stmt`: IndexStmt structure that will be modified with reuse information if the index is compatible

## Dependencies
- Functions called/Symbols referenced:
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)
  - [index_open](../i/index_open.md)
  - [index_close](../i/index_close.md)
  - RELKIND_PARTITIONED_INDEX
- Called from:
  - [ATPostAlterTypeParse](../A/ATPostAlterTypeParse.md)

## Notes and Other Information
- This function is part of the column type alteration optimization process
- Storage reuse is only applicable to non-partitioned indexes
- The function uses NoLock when opening/closing the index since appropriate locks should already be held by the calling context
- The oldNumber, oldCreateSubid, and oldFirstRelfilelocatorSubid fields are set in the IndexStmt to enable the storage reuse mechanism