# LogicalTapeClose

## Location
[src/backend/utils/sort/logtape.c:733-749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L733-L749)

## Overview
Closes a LogicalTape by freeing its associated memory resources, including the I/O buffer and the tape structure itself.

## Definition

```c
void
LogicalTapeClose(LogicalTape *lt)
```
## Detailed Description
The `LogicalTapeClose` function is responsible for properly cleaning up and deallocating a LogicalTape structure when it's no longer needed. The function performs a simple but important cleanup operation by freeing the tape's I/O buffer (if allocated) and then freeing the tape structure itself.

An important characteristic of this function is that it does NOT return any disk blocks used by the tape back to the free list. This means that the space used by the tape's data remains allocated until the tape is fully read to completion. The current PostgreSQL design expects tapes to be read completely before being closed, which naturally reclaims the space during the read process.

This design choice reflects the typical usage pattern in external sorting operations where tapes are either written completely and then read completely, or are temporary structures that don't require fine-grained space reclamation.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape structure to be closed and freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (for memory deallocation)
  - [LogicalTape](LogicalTape.md) (structure type)
- Called from (representative examples):
  - [tuplesort_gettuple_common](../t/tuplesort_gettuple_common.md)
  - [mergeruns](../m/mergeruns.md)
  - [agg_refill_hash_table](../a/agg_refill_hash_table.md)

## Notes and Other Information
- Does not return disk blocks to the free list - caller must ensure tape is fully read first
- Uses conditional freeing for the buffer (checks if buffer exists before freeing)
- Simple cleanup function with no error handling needed
- Current PostgreSQL usage patterns ensure tapes are fully read before closure
- Memory cleanup is straightforward since tapes don't maintain complex internal state requiring special cleanup

## Simplified Source

```c
void LogicalTapeClose(LogicalTape *lt) {
    // Free I/O buffer if it exists
    if (lt->buffer) {
        pfree(lt->buffer);
    }

    // Free the tape structure itself
    pfree(lt);
}
```