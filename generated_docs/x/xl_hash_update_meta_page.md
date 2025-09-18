# xl_hash_update_meta_page

## Location
[src/include/access/hash_xlog.h:201-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L201-L204)

## Overview
The xl_hash_update_meta_page struct represents the WAL record data for hash index metapage update operations, used to log changes to the hash index's metadata page.

## Definition


## Detailed Description
This structure contains the necessary information to perform or replay hash index metapage update operations during WAL recovery. The metapage update operation modifies the metadata information of a hash index, particularly the tuple count statistic. This is typically used during bulk delete operations or other maintenance tasks that affect the overall statistics of the hash index.

The operation involves 1 backup block:
- Backup Blk 0: meta page

## Parameters / Member Variables
- : The updated number of tuples in the hash index, stored as a double precision floating-point value for statistical purposes

## Dependencies
- Functions called/Symbols referenced:
  - double (type)
- Called from (representative examples):
  - [hashbulkdelete](../h/hashbulkdelete.md) (hash bulk deletion function)
  - [hash_xlog_update_meta_page](../h/hash_xlog_update_meta_page.md) (WAL replay function for metapage updates)
  - [hash_desc](../h/hash_desc.md) (hash WAL record description function)
  - SizeOfHashUpdateMetaPage (macro for calculating structure size)

## Notes and Other Information
- This is specifically used for XLOG_HASH_UPDATE_META_PAGE WAL record type
- The ntuples field is used to maintain accurate statistics about the number of tuples in the hash index
- This information is important for query planning and index maintenance decisions
- The use of double precision allows for storing approximate tuple counts for very large indexes
- Part of PostgreSQL's hash index access method implementation for maintaining metadata consistency
- Critical for ensuring proper recovery of hash index metadata after a crash
- Defined in src/include/access/hash_xlog.h at lines 201-204