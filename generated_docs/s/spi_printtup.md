# spi_printtup

## Location
[src/backend/executor/spi.c:2171-2220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2171-L2220)

## Overview
spi_printtup is a callback function that stores tuple results retrieved by the Executor into the SPITupleTable of the current SPI procedure.

## Definition

```c
bool
spi_printtup(TupleTableSlot *slot, DestReceiver *self)
```
## Detailed Description
The spi_printtup function serves as a destination receiver callback used within the Server Programming Interface (SPI) framework. When executing SQL commands through SPI, the Executor calls this function to store each result tuple into the current SPI connection's tuple table. The function handles dynamic memory allocation for the tuple array, doubling its size when needed to accommodate growing result sets.

The function operates within the memory context of the SPI tuple table to ensure proper memory management and cleanup. It validates that an SPI connection is active and that the tuple table is properly initialized before attempting to store tuples.

## Parameters / Member Variables
- `*slot`: TupleTableSlot containing the tuple data to be stored
- `*self`: DestReceiver pointer (unused in this implementation but required by the callback interface)
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [repalloc_huge](../r/repalloc_huge.md)
  - [ExecCopySlotHeapTuple](../E/ExecCopySlotHeapTuple.md)
  - elog
- Called from (representative examples):
  - Used as callback function in DestReceiver operations
  - Referenced in printtup.h header file

## Notes and Other Information
- The function doubles the tuple array size when capacity is exceeded, using repalloc_huge for large allocations
- Operates within the tuple table's memory context to ensure proper cleanup
- Returns true on successful tuple storage
- Validates SPI connection state and tuple table initialization before processing
- Each stored tuple is a heap tuple copy created from the slot data