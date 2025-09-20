# MdfdVec

## Location
[src/backend/storage/smgr/md.c:84-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/smgr/md.c#L84-L89)

## Overview
MdfdVec is a structure that represents a single segment file descriptor in PostgreSQL's magnetic disk (md) storage manager, containing the virtual file descriptor and segment number information for efficient file operations.

## Definition

```c
typedef struct _MdfdVec
{
	File		mdfd_vfd;		/* fd number in fd.c's pool */
	BlockNumber mdfd_segno;		/* segment number, from 0 */
} MdfdVec;
```
## Detailed Description
MdfdVec is a fundamental data structure in PostgreSQL's storage management system, specifically within the magnetic disk (md) storage manager located in src/backend/storage/smgr/md.c. This structure serves as a descriptor for individual segment files that make up larger PostgreSQL relation files.

PostgreSQL splits large relation files into multiple 1GB segments to work around file system limitations and improve manageability. Each MdfdVec structure tracks one of these segments, maintaining both the virtual file descriptor (managed by PostgreSQL's fd.c virtual file descriptor pool) and the segment number within the relation.

The structure is designed for efficient segment file management, allowing the storage manager to quickly locate and operate on specific segments of a relation. It's used throughout the md.c storage manager implementation for operations like reading, writing, extending, and truncating relation files.

## Parameters / Member Variables
- : Virtual file descriptor number from PostgreSQL's fd.c pool system, which manages actual OS file descriptors efficiently
- : Zero-based segment number within the relation file, used to identify which 1GB segment this descriptor represents

## Dependencies
- Functions called/Symbols referenced:
  - File (type from src/include/storage/fd.h)
  - BlockNumber (PostgreSQL block number type)
- Called from (representative examples):
  - [mdcreate](../m/mdcreate.md)
  - mdextend
  - mdzeroextend
  - mdopenfork
  - [mdclose](../m/mdclose.md)
  - [mdprefetch](../m/mdprefetch.md)
  - [mdreadv](../m/mdreadv.md)
  - [mdwritev](../m/mdwritev.md)
  - [mdwriteback](../m/mdwriteback.md)
  - [mdnblocks](../m/mdnblocks.md)
  - [mdtruncate](../m/mdtruncate.md)
  - [mdregistersync](../m/mdregistersync.md)
  - [mdimmedsync](../m/mdimmedsync.md)
  - [register_dirty_segment](../r/register_dirty_segment.md)
  - [_fdvec_resize](../f/_fdvec_resize.md)
  - [_mdfd_segpath](../m/_mdfd_segpath.md)
  - [_mdfd_openseg](../m/_mdfd_openseg.md)
  - [_mdfd_getseg](../m/_mdfd_getseg.md)
  - [_mdnblocks](../m/_mdnblocks.md)

## Notes and Other Information
- [MdfdVec](MdfdVec.md) objects are allocated in the MdCxt memory context for centralized memory management
- This structure is central to PostgreSQL's approach of handling large files by splitting them into manageable segments
- The virtual file descriptor system (fd.c) allows PostgreSQL to manage more files than the OS would normally allow by recycling file descriptors
- Each segment typically represents 1GB of data (RELSEG_SIZE blocks), though the last segment may be smaller
- The structure is used in arrays (fdvec) to represent all segments of a relation fork
- File operations on segments are coordinated through this structure to maintain data integrity and performance