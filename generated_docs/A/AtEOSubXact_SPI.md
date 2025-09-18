# AtEOSubXact_SPI

## Location
src/backend/executor/spi.c: 482 - 580

## Overview
AtEOSubXact_SPI cleans up SPI state at subtransaction commit or abort, handling proper cleanup of SPI connections and memory contexts that belong to the ending subtransaction.

## Definition
```c
void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid)
```

## Detailed Description
This function is called at the end of a subtransaction to clean up SPI state specific to that subtransaction. It operates differently from AtEOXact_SPI by being more selective about which connections and resources to clean up.

During commit, the function pops SPI stack entries that belong to the current subtransaction and issues warnings for any unclosed connections. During abort, it performs more extensive cleanup including:
- Explicitly deleting memory contexts (execCxt and procCxt) for connections from the current subtransaction
- Resetting executor state if operations were started within the current subtransaction
- Cleaning up tuple tables created within the subtransaction to prevent memory leakage

The function uses subtransaction IDs to determine which resources belong to the ending subtransaction and need cleanup.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether this is subtransaction commit (true) or abort (false)
- `mySubid`: The SubTransactionId of the subtransaction being ended

## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId (type)
  - _SPI_connection (struct type)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (memory management function)
  - [MemoryContextReset](../M/MemoryContextReset.md) (memory management function)
  - [slist_mutable_iter](../s/slist_mutable_iter.md) (list iteration type)
  - slist_foreach_modify (list iteration macro)
  - slist_container (list container macro)
  - [slist_delete_current](../s/slist_delete_current.md) (list deletion macro)
  - [SPITupleTable](../S/SPITupleTable.md) (struct type)
  - InvalidSubTransactionId (constant)
  - ereport (error reporting function)

- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (src/backend/access/transam/xact.c:5128)
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5292)

## Notes and Other Information
- More complex than AtEOXact_SPI due to need to handle partial cleanup within ongoing transactions
- Explicitly manages memory contexts rather than relying on automatic cleanup
- Uses subtransaction IDs to precisely identify which resources need cleanup
- Includes optimization to avoid O(N²) operations when cleaning up tuple tables
- Critical for preventing memory leaks in subtransaction scenarios
- Located in src/backend/executor/spi.c:482-580