# xl_hash_delete

## Location
src/include/access/hash_xlog.h: 184 - 190

## Overview
The xl_hash_delete struct represents the WAL record data for hash index tuple deletion operations, used to log the removal of index tuples from hash index pages.

## Definition


## Detailed Description
This structure contains the metadata necessary to perform or replay hash index tuple deletion operations during WAL recovery. The deletion operation removes index tuples from a hash index page and may also clear dead tuple markings. This is part of PostgreSQL's hash index maintenance operations, typically occurring during index cleanup or vacuum operations.

The operation involves up to 2 backup blocks:
- Backup Blk 0: primary bucket page
- Backup Blk 1: page from which tuples are deleted

## Parameters / Member Variables
- : Boolean flag indicating whether this operation clears the LH_PAGE_HAS_DEAD_TUPLES flag on the page
- : Boolean flag indicating whether the deletion operation is being performed on the primary bucket page

## Dependencies
- Functions called/Symbols referenced:
  - [bool](../b/bool.md) (type)
- Called from (representative examples):
  - [hashbucketcleanup](../h/hashbucketcleanup.md) (hash bucket cleanup function)
  - [hash_xlog_delete](../h/hash_xlog_delete.md) (WAL replay function for deletions)
  - [hash_desc](../h/hash_desc.md) (hash WAL record description function)
  - SizeOfHashDelete (macro for calculating structure size)

## Notes and Other Information
- This is specifically used for XLOG_HASH_DELETE WAL record type
- The clear_dead_marking flag is important for maintaining the LH_PAGE_HAS_DEAD_TUPLES page header flag, which tracks whether a page contains dead tuples
- The is_primary_bucket_page flag helps distinguish between operations on primary bucket pages versus overflow pages
- Part of PostgreSQL's hash index access method implementation for maintaining index consistency
- Critical for ensuring proper recovery of hash index deletion operations after a crash
- Defined in src/include/access/hash_xlog.h at lines 184-190