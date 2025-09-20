# mdinit

## Location
[src/backend/storage/smgr/md.c:158-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L158-L170)

## Overview
mdinit initializes the private memory context for the magnetic disk storage manager, setting up the foundational memory management for all MD operations.

## Definition
void mdinit(void)

## Detailed Description
This initialization function creates a dedicated memory context called "MdSmgr" for the magnetic disk storage manager. The memory context is created as a child of TopMemoryContext using the AllocSet algorithm with default sizing parameters. This context will be used to manage all dynamic memory allocations related to magnetic disk operations, providing proper memory isolation and cleanup capabilities for the storage manager.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (creates the memory context)
  - TopMemoryContext (parent memory context)
  - ALLOCSET_DEFAULT_SIZES (default memory context sizing)

- Called from (representative examples):
  - Declared in src/include/storage/md.h for external usage
  - Typically called during storage manager initialization

## Notes and Other Information
- This function must be called before any other MD operations to ensure proper memory management
- The created MdCxt memory context is used throughout the magnetic disk storage manager for allocations
- Uses AllocSet algorithm which provides efficient allocation and deallocation for variable-sized chunks
- The memory context name "MdSmgr" helps with debugging and memory usage tracking
- Part of the storage manager initialization sequence in PostgreSQL