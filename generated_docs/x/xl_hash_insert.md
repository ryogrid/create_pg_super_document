# xl_hash_insert

## Location
[src/include/access/hash_xlog.h:62-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/hash_xlog.h#L62-L65)

## Overview
A PostgreSQL WAL (Write-Ahead Logging) record structure that represents the data needed to replay a simple hash index insertion operation during recovery.

## Definition

```c
typedef struct xl_hash_insert
{
	OffsetNumber offnum;
} xl_hash_insert;
```
## Detailed Description
The  structure is used for  WAL records, which log simple hash index insertions that do not involve page splits. This minimal structure contains only the essential information needed to replay the insertion during crash recovery. The actual tuple data is stored in the backup blocks rather than in this structure itself.

During WAL replay, this record works in conjunction with two backup blocks:
- Backup Block 0: The original page containing the inserted tuple data
- Backup Block 1: The metapage containing HashMetaPageData

The simplicity of this structure reflects PostgreSQL's efficient WAL design - most of the insertion details are preserved in the backup blocks, while this record only needs to store the specific offset where the tuple was inserted.

## Parameters / Member Variables
- `offnum`: The offset number on the page where the tuple was inserted, used during WAL replay to determine the exact location of the inserted item
## Dependencies
- Functions called/Symbols referenced:
  - OffsetNumber (type)
- Called from (representative examples):
  - [hash_xlog_insert](../h/hash_xlog_insert.md) (WAL replay function)
  - [_hash_doinsert](../h/_hash_doinsert.md) (hash insertion implementation)
  - [hash_desc](../h/hash_desc.md) (WAL record description function)
  - SizeOfHashInsert (macro for size calculation)

## Notes and Other Information
- This structure is specifically for simple insertions without page splits
- The actual tuple data is not stored in this record but in the associated backup blocks
- Part of PostgreSQL's hash index WAL logging infrastructure
- Defined in src/include/access/hash_xlog.h:62-65
- Used exclusively for crash recovery and replication purposes