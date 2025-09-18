# read_seq_tuple

## Location
src/backend/commands/sequence.c: 1190 - 1256

## Overview
Reads and locks the sequence data tuple from a sequence relation's page buffer, returning a pointer to the sequence data structure.

## Definition
```c
static Form_pg_sequence_data read_seq_tuple(Relation rel, Buffer *buf, HeapTuple seqdatatuple)
```

## Detailed Description
This function provides low-level access to sequence data stored in PostgreSQL heap pages. It reads the sequence relation's first (and only) page, acquires an exclusive lock on the buffer, and retrieves the sequence data tuple. The function also validates the sequence magic number to ensure data integrity and handles cleanup of legacy xmax values that could cause transaction log access issues.

The function performs several critical operations: it reads the buffer for page 0 of the sequence relation, locks it exclusively to prevent concurrent access, validates the sequence magic number stored in the page's special area, locates the sequence data tuple using the first offset number, and initializes the HeapTuple structure with the tuple data and length.

Additionally, it includes compatibility code to clean up sequence tuples that may have been modified by SELECT FOR UPDATE in previous PostgreSQL versions, which could leave non-frozen XIDs that eventually cause problems.

## Parameters / Member Variables
- `rel`: The opened sequence relation to read from
- `buf`: Output parameter that receives the reference to the pinned and exclusively locked buffer
- `seqdatatuple`: Output parameter pointing to a local HeapTupleData variable that receives the tuple reference

## Dependencies
- Functions called/Symbols referenced:
  - ItemId (page item identifier)
  - [sequence_magic](../s/sequence_magic.md) (sequence page magic number structure)
  - Form_pg_sequence_data (sequence data structure)
  - [ReadBuffer](../R/ReadBuffer.md) (reads a relation page into buffer pool)
  - BUFFER_LOCK_EXCLUSIVE (exclusive buffer lock mode)
  - [PageGetSpecialPointer](../P/PageGetSpecialPointer.md) (gets page special area pointer)
  - SEQ_MAGIC (sequence magic number constant)
  - [PageGetItemId](../P/PageGetItemId.md) (gets item identifier from page)
  - FirstOffsetNumber (first valid offset number)
  - ItemIdIsNormal (checks if item ID is normal)
  - HeapTupleHeader (heap tuple header structure)
  - [PageGetItem](../P/PageGetItem.md) (gets item data from page)
  - ItemIdGetLength (gets item length)
  - Various heap tuple manipulation functions for xmax cleanup
- Called from (representative examples):
  - [ResetSequence](../R/ResetSequence.md)
  - [AlterSequence](../A/AlterSequence.md)
  - [SequenceChangePersistence](../S/SequenceChangePersistence.md)
  - [nextval_internal](../n/nextval_internal.md)
  - [do_setval](../d/do_setval.md)
  - [pg_sequence_last_value](../p/pg_sequence_last_value.md)

## Notes and Other Information
- Always reads page 0 since sequences only use a single page
- Acquires exclusive lock to ensure atomic access to sequence data
- Includes legacy cleanup code for sequences modified by SELECT FOR UPDATE in older PostgreSQL versions
- The xmax cleanup is treated as a hint bit update and is not WAL-logged
- Magic number validation prevents access to corrupted sequence pages
- This is a static function internal to src/backend/commands/sequence.c