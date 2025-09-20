# _MdfdVec

## Location
[src/backend/storage/smgr/md.c:80-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L80-L83)

## Overview
The  structure represents a single segment file descriptor in PostgreSQL's magnetic disk storage manager, tracking both the file descriptor and segment number for managing large relations that are split across multiple physical files.

## Definition

```c
typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;
```
## Detailed Description
The  structure is a fundamental component of PostgreSQL's magnetic disk storage manager (md.c). It serves as a descriptor for individual segment files that comprise a relation. PostgreSQL breaks large relations into multiple segment files to overcome operating system file size limitations (often 2GB). Each  entry represents one such segment file.

The structure is designed to support PostgreSQL's segmented file architecture where:
- Relations are split into segments of RELSEG_SIZE blocks each
- Each segment is tracked by its own  entry
- The structure maintains both the virtual file descriptor and the segment number for efficient access
- All  objects are allocated in the MdCxt memory context for centralized memory management

The magnetic disk storage manager maintains arrays of these structures in the SMgrRelation to track open file descriptors for each fork of a relation.

## Parameters / Member Variables
- : Virtual file descriptor number from fd.c's file descriptor pool, used to reference the actual open file
- : Zero-based segment number identifying which segment of the relation this descriptor represents

## Dependencies
- Functions called/Symbols referenced:
  - File (typedef from fd.c)
  - BlockNumber (typedef)
- Used by:
  - SMgrRelationData.md_seg_fds (arrays of _MdfdVec pointers for each fork)

## Notes and Other Information
- All  objects are allocated in the MdCxt memory context for efficient memory management
- The structure supports PostgreSQL's strategy of breaking large relations into multiple segment files
- Segment files follow a specific naming pattern and size constraints (RELSEG_SIZE blocks)
- The storage manager can have "active" segments (containing data) and "inactive" segments (zero-sized placeholders)
- File descriptors are managed per-fork, allowing different forks of a relation to have different numbers of open segments
- The structure is internal to the md.c storage manager and not exposed to higher-level PostgreSQL components