# AtEOSubXact_SPI

## Location
[src/backend/executor/spi.c:482-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L482-L580)

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

## Simplified Source

```c
// Simplified version of AtEOSubXact_SPI
void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid) {
    bool found = false;

    // Step 1: Clean up SPI connections from current subtransaction
    while (_SPI_connected >= 0) {
        _SPI_connection *connection = &(_SPI_stack[_SPI_connected]);

        // Stop if this connection is from a different subtransaction
        if (connection->connectSubid != mySubid || connection->internal_xact)
            break;

        found = true;

        // Clean up memory contexts for this connection
        if (connection->execCxt) {
            MemoryContextDelete(connection->execCxt);
            connection->execCxt = NULL;
        }
        if (connection->procCxt) {
            MemoryContextDelete(connection->procCxt);
            connection->procCxt = NULL;
        }

        // Restore global SPI state from outer connection
        SPI_processed = connection->outer_processed;
        SPI_tuptable = connection->outer_tuptable;
        SPI_result = connection->outer_result;

        // Pop this connection from the stack
        _SPI_connected--;
        _SPI_current = (_SPI_connected < 0) ? NULL : &(_SPI_stack[_SPI_connected]);
    }

    // Step 2: Warn if unclosed connections found during commit
    if (found && isCommit) {
        ereport(WARNING, "subtransaction left non-empty SPI stack");
    }

    // Step 3: Additional cleanup during abort for surrounding context
    if (_SPI_current && !isCommit) {
        // Reset executor state if started within this subtransaction
        if (_SPI_current->execSubid >= mySubid) {
            _SPI_current->execSubid = InvalidSubTransactionId;
            MemoryContextReset(_SPI_current->execCxt);
        }

        // Clean up tuple tables created within this subtransaction
        slist_mutable_iter siter;
        slist_foreach_modify(siter, &_SPI_current->tuptables) {
            SPITupleTable *tuptable = slist_container(SPITupleTable, next, siter.cur);

            if (tuptable->subid >= mySubid) {
                // Remove and free the tuple table
                slist_delete_current(&siter);
                if (tuptable == _SPI_current->tuptable)
                    _SPI_current->tuptable = NULL;
                if (tuptable == SPI_tuptable)
                    SPI_tuptable = NULL;
                MemoryContextDelete(tuptable->tuptabcxt);
            }
        }
    }
}
```

Key simplifications made:
- Removed detailed error code and hint message from ereport for brevity
- Added clear step-by-step comments to explain the three main phases
- Simplified complex conditional logic into clearer flow
- Consolidated memory context cleanup into a single block
- Removed detailed comments about O(N²) optimization while preserving the logic
- Used more descriptive variable organization and flow control