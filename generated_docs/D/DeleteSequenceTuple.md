# DeleteSequenceTuple

## Location
[src/backend/commands/sequence.c:570-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/sequence.c#L570-L592)

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
  - [table_open](../t/table_open.md) (on SequenceRelationId with RowExclusiveLock)
  - [SearchSysCache1](../S/SearchSysCache1.md) (with SEQRELID cache)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog (ERROR level)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (in dependency.c)

## Notes and Other Information
- Part of the dependency management system for sequence deletion
- Uses the SEQRELID system cache for efficient sequence tuple lookup
- Errors out if the sequence tuple is not found in the catalog, indicating a consistency problem
- Acquires RowExclusiveLock on the pg_sequence catalog during the deletion operation
- Essential for maintaining catalog consistency during sequence cleanup operations

## Simplified Source

```c
void
DeleteSequenceTuple(Oid relid)
{
    Relation rel;
    HeapTuple tuple;

    // Open the pg_sequence catalog with exclusive lock
    rel = table_open(SequenceRelationId, RowExclusiveLock);

    // Find the sequence tuple by relation OID
    tuple = SearchSysCache1(SEQRELID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for sequence %u", relid);

    // Delete the sequence metadata tuple
    CatalogTupleDelete(rel, &tuple->t_self);

    // Cleanup
    ReleaseSysCache(tuple);
    table_close(rel, RowExclusiveLock);
}
```