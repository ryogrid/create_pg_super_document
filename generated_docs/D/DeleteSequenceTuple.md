# DeleteSequenceTuple

## Location
src/backend/commands/sequence.c: 570 - 592

## Overview
Removes a sequence's catalog entry from the pg_sequence system catalog when a sequence is being dropped.

## Definition
```c
void DeleteSequenceTuple(Oid relid)
```

## Detailed Description
This function handles the deletion of a sequence's metadata tuple from the pg_sequence system catalog. It is called as part of the sequence deletion process during DROP SEQUENCE operations. The function performs a catalog lookup to find the sequence's tuple and then removes it from the system catalog, ensuring that all sequence-related metadata is properly cleaned up when a sequence is dropped.

The function operates on the SequenceRelationId catalog (pg_sequence) and uses the standard PostgreSQL catalog deletion mechanisms to maintain system catalog integrity.

## Parameters / Member Variables
- `relid`: The OID of the sequence relation whose catalog entry should be deleted

## Dependencies
- Functions called/Symbols referenced:
  - table_open (on SequenceRelationId with RowExclusiveLock)
  - SearchSysCache1 (with SEQRELID cache)
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - elog (ERROR level)
  - CatalogTupleDelete
  - ReleaseSysCache
  - table_close
- Called from (representative examples):
  - doDeletion (in dependency.c)

## Notes and Other Information
- Part of the dependency management system for sequence deletion
- Uses the SEQRELID system cache for efficient sequence tuple lookup
- Errors out if the sequence tuple is not found in the catalog, indicating a consistency problem
- Acquires RowExclusiveLock on the pg_sequence catalog during the deletion operation
- Essential for maintaining catalog consistency during sequence cleanup operations