# smgrexists

## Location
[src/backend/storage/smgr/smgr.c:398-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/smgr.c#L398-L410)

## Overview
Checks whether the underlying file for a specific fork of a relation exists on disk.

## Definition

```c
bool
smgrexists(SMgrRelation reln, ForkNumber forknum)
```
## Detailed Description
This function determines if the physical file corresponding to a specific fork of a relation exists in the storage system. It acts as a dispatcher that delegates the actual existence check to the appropriate storage manager implementation through the smgrsw function table. The function returns true if the file exists, false otherwise.

The function uses the storage manager switch (smgrsw) to call the appropriate exists method based on the relation's storage manager type (reln->smgr_which). This abstraction allows PostgreSQL to support different storage managers while providing a uniform interface for existence checking.

## Parameters / Member Variables
- `reln`: SMgrRelation pointer representing the relation to check
- `forknum`: ForkNumber indicating which fork of the relation to check (main, FSM, VM, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - smgrsw (storage manager switch table)
  - SMgrRelation (relation structure type)
  - [ForkNumber](../F/ForkNumber.md) (fork identifier type)
- Called from (representative examples):
  - [heapam_relation_copy_data](../h/heapam_relation_copy_data.md) (heap access method)
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md) (visibility map operations)
  - [vm_readbuf](../v/vm_readbuf.md) (visibility map buffer management)
  - [XLogPrefetcherNextBlock](../X/XLogPrefetcherNextBlock.md) (WAL prefetching)
  - [index_build](../i/index_build.md) (index construction)
  - [RelationTruncate](../R/RelationTruncate.md) (relation truncation operations)
  - [ExtendBufferedRelTo](../E/ExtendBufferedRelTo.md) (buffer management)
  - [CreateAndCopyRelationData](../C/CreateAndCopyRelationData.md) (relation data copying)

## Notes and Other Information
- This is a fundamental storage layer function used throughout PostgreSQL for checking file existence before operations
- The function is used extensively in buffer management, index operations, and relation maintenance
- Commonly used before attempting file operations to avoid errors or to determine if initialization is needed
- Part of the storage manager abstraction layer that allows different storage implementations
- Located in src/backend/storage/smgr/smgr.c:398-410

## Simplified Source

```c
bool smgrexists(SMgrRelation reln, ForkNumber forknum)
{
    return smgrsw[reln->smgr_which].smgr_exists(reln, forknum);
}
```

This function checks if the physical file for a specific fork of a relation exists. It dispatches to the appropriate storage manager implementation via the smgrsw function table based on the relation's storage manager type.