# ResolveCminCmaxDuringDecoding

## Location
[src/backend/replication/logical/reorderbuffer.c:5404-5477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L5404-L5477)

## Overview
ResolveCminCmaxDuringDecoding looks up the command IDs (cmin/cmax) of a tuple during logical decoding when combo CIDs cannot be relied upon.

## Definition

```c
bool
ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data,
							  Snapshot snapshot,
							  HeapTuple htup, Buffer buffer,
							  CommandId *cmin, CommandId *cmax)
```
## Detailed Description
ResolveCminCmaxDuringDecoding is a critical function in PostgreSQL's logical replication system that resolves command ID information for tuples during logical decoding. During normal transaction processing, PostgreSQL uses combo CIDs to efficiently track multiple commands within a transaction. However, during logical decoding, these combo CIDs are not available, necessitating an alternative mechanism.

The function works by:
1. Creating a lookup key from the tuple's location (relation file locator, block number, and item pointer)
2. Searching a hash table (tuplecid_data) for previously stored command ID information
3. If no mapping is found initially, it attempts to update logical mappings through UpdateLogicalMappings, which handles cases where tables have been rewritten
4. Returns the stored cmin and cmax values if found, or false if the tuple's command IDs cannot be resolved

This function is essential for maintaining MVCC (Multi-Version Concurrency Control) semantics during logical replication, ensuring that tuple visibility is correctly determined based on transaction boundaries and command sequences.

## Parameters / Member Variables
- : Hash table containing tuple command ID mappings; NULL when streaming in-progress transactions
- : Snapshot context providing transaction visibility information
- : The heap tuple for which command IDs need to be resolved
- : Buffer containing the tuple, used to extract relation file locator information
- : Output parameter for the minimum command ID (when tuple was created/modified)
- : Output parameter for the maximum command ID (when tuple was deleted/updated)

## Dependencies
- Functions called/Symbols referenced:
  - BufferIsLocal
  - [BufferGetTag](../B/BufferGetTag.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [hash_search](../h/hash_search.md)
  - [UpdateLogicalMappings](../U/UpdateLogicalMappings.md)
  - [ReorderBufferTupleCidKey](ReorderBufferTupleCidKey.md)
  - [ReorderBufferTupleCidEnt](ReorderBufferTupleCidEnt.md)
  - MAIN_FORKNUM
  - HASH_FIND
- Called from (representative examples):
  - [HeapTupleSatisfiesHistoricMVCC](../H/HeapTupleSatisfiesHistoricMVCC.md)
  - HeapScanIsValid

## Notes and Other Information
- Returns false when tuplecid_data is NULL, which occurs when streaming in-progress transactions where CID resolution may encounter tuples before their commands are decoded
- Uses a restart mechanism to handle table rewrites: if initial lookup fails, it updates logical mappings once and retries
- Only performs mapping updates once per call to avoid infinite loops and because relation locks prevent new mappings during execution
- Asserts that tuples are only in the main fork and validates block number consistency
- The function handles the complex case where logical decoding needs to reconstruct command ID information that would normally be available through combo CIDs
- Critical for maintaining transactional consistency in logical replication scenarios

## Simplified Source

```c
bool
ResolveCminCmaxDuringDecoding(HTAB *tuplecid_data,
                              Snapshot snapshot,
                              HeapTuple htup, Buffer buffer,
                              CommandId *cmin, CommandId *cmax)
{
    ReorderBufferTupleCidKey key;
    ReorderBufferTupleCidEnt *ent;
    ForkNumber forkno;
    BlockNumber blockno;
    bool updated_mapping = false;

    // Return false if no CID data available (streaming case)
    if (tuplecid_data == NULL)
        return false;

    // Build lookup key from tuple location
    memset(&key, 0, sizeof(key));

    Assert(!BufferIsLocal(buffer));

    BufferGetTag(buffer, &key.rlocator, &forkno, &blockno);
    Assert(forkno == MAIN_FORKNUM);
    Assert(blockno == ItemPointerGetBlockNumber(&htup->t_self));

    ItemPointerCopy(&htup->t_self, &key.tid);

restart:
    // Search for tuple CID mapping
    ent = (ReorderBufferTupleCidEnt *) hash_search(tuplecid_data, &key, HASH_FIND, NULL);

    // If not found and haven't updated mappings yet, try updating
    if (ent == NULL && !updated_mapping) {
        UpdateLogicalMappings(tuplecid_data, htup->t_tableOid, snapshot);
        updated_mapping = true;
        goto restart;
    } else if (ent == NULL) {
        return false;
    }

    // Return the found command IDs
    if (cmin)
        *cmin = ent->cmin;
    if (cmax)
        *cmax = ent->cmax;

    return true;
}
```