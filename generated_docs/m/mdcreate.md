# mdcreate

## Location
src/backend/storage/smgr/md.c: 190 - 306

## Overview
mdcreate creates a new physical file for a relation fork on magnetic disk, handling tablespace setup, file creation, and initialization of the storage manager structures.

## Definition
void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo)

## Detailed Description
This function creates a new relation file on disk with comprehensive error handling and proper initialization. It first ensures the target tablespace directory structure exists by calling TablespaceCreateDbspace. The function then creates the physical file using exclusive creation flags (O_CREAT | O_EXCL) to prevent overwriting existing files. If running in redo mode and the file already exists, it attempts to open the existing file instead. After successful creation, it initializes the file descriptor vector, sets up the MdfdVec structure, and registers the segment as dirty for non-temporary relations to ensure proper cleanup tracking.

## Parameters / Member Variables
- : SMgrRelation representing the storage manager relation to create
- : ForkNumber specifying which fork to create (main, FSM, VM, etc.)
- : bool indicating whether this is a redo operation (allows existing files)

## Dependencies
- Functions called/Symbols referenced:
  - [TablespaceCreateDbspace](../T/TablespaceCreateDbspace.md) (ensures tablespace directory exists)
  - relpath (constructs the file path)
  - PathNameOpenFile (opens/creates the file)
  - [_mdfd_open_flags](_mdfd_open_flags.md) (gets appropriate file open flags)
  - [_fdvec_resize](../f/_fdvec_resize.md) (resizes the file descriptor vector)
  - SmgrIsTemp (checks if relation is temporary)
  - [register_dirty_segment](../r/register_dirty_segment.md) (tracks dirty segments for cleanup)
  - [pfree](../p/pfree.md) (frees allocated memory)

- Called from (representative examples):
  - Declared in src/include/storage/md.h for external usage
  - Used by higher-level storage management during table creation

## Notes and Other Information
- Contains a module layering violation noted in comments regarding TablespaceCreateDbspace placement
- Uses O_EXCL flag to prevent accidental overwriting of existing files
- Handles redo operations gracefully by allowing existing files to be opened
- Automatically creates per-database subdirectories in tablespaces as needed
- Registers non-temporary segments as dirty to ensure proper cleanup during crashes
- Part of the core file creation mechanism in PostgreSQL's magnetic disk storage manager