# xl_hash_init_meta_page

## Location
[src/include/access/hash_xlog.h:216-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L216-L221)

## Overview
The xl_hash_init_meta_page struct represents the WAL record data for hash index metapage initialization operations, used to log the creation and initial setup of a hash index's metadata page.

## Definition

```c
typedef struct xl_hash_init_meta_page
{
	double		num_tuples;
	RegProcedure procid;
	uint16		ffactor;
} xl_hash_init_meta_page;
```
## Detailed Description
This structure contains the necessary information to perform or replay hash index metapage initialization operations during WAL recovery. The metapage initialization occurs when a new hash index is created, setting up the fundamental metadata that controls the hash index's behavior and structure. This includes the hash function to use, the fill factor, and the initial tuple count estimate.

The operation involves 1 backup block:
- Backup Blk 0: meta page

## Parameters / Member Variables
- : Initial estimate of the number of tuples expected in the hash index, stored as a double precision floating-point value
- : The OID of the hash function procedure to be used for this hash index, stored as a RegProcedure type
- : The fill factor for the hash index, indicating how full each bucket page should be before splitting occurs

## Dependencies
- Functions called/Symbols referenced:
  - RegProcedure (type for storing procedure OIDs)
  - double (type)
  - uint16 (type)
- Called from (representative examples):
  - [hash_xlog_init_meta_page](../h/hash_xlog_init_meta_page.md) (WAL replay function for metapage initialization)
  - [_hash_init](../h/_hash_init.md) (hash index initialization function)
  - [hash_desc](../h/hash_desc.md) (hash WAL record description function)
  - SizeOfHashInitMetaPage (macro for calculating structure size)

## Notes and Other Information
- This is specifically used for XLOG_HASH_INIT_META_PAGE WAL record type
- The procid field specifies which hash function will be used for distributing tuples across buckets
- The ffactor (fill factor) is crucial for hash index performance, controlling when bucket splits occur
- The num_tuples field provides an initial estimate that can be used for planning purposes
- Part of PostgreSQL's hash index access method implementation for creating new indexes
- Critical for ensuring proper recovery of hash index creation operations after a crash
- This operation only occurs during initial index creation, not during normal index maintenance
- Defined in src/include/access/hash_xlog.h at lines 216-221