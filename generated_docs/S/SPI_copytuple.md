# SPI_copytuple

## Location
[src/backend/executor/spi.c:1047-1073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L1047-L1073)

## Overview
Creates a copy of a HeapTuple in the SPI procedures saved memory context, allowing the tuple to persist beyond the current query execution context.

## Definition
```c
HeapTuple SPI_copytuple(HeapTuple tuple)
```

## Detailed Description
SPI_copytuple creates a deep copy of a HeapTuple by copying it into the SPI procedures saved memory context. This function is essential when you need to preserve tuple data beyond the lifetime of the current query execution context. Without copying, tuples returned from SPI operations may become invalid when the querys memory context is destroyed. The function switches to the saved memory context before performing the copy operation using heap_copytuple, ensuring the copied tuple will remain valid for the duration of the SPI procedure. This is particularly important when building result sets or caching tuple data for later use.

## Parameters / Member Variables
- `tuple`: Pointer to the HeapTuple to be copied

## Dependencies
- Functions called/Symbols referenced:
  - SPI_ERROR_ARGUMENT
  - SPI_ERROR_UNCONNECTED
  - [heap_copytuple](../h/heap_copytuple.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - _SPI_current (global SPI connection state)
- Called from (representative examples):
  - Functions using SPI_OPT_NONATOMIC option

## Notes and Other Information
- Returns a new HeapTuple on success, NULL on failure with SPI_result set to error code
- Requires an active SPI connection (SPI_connect must have been called)
- The copied tuple is allocated in the procedures saved memory context for persistence
- The original tuple remains unchanged and retains its original memory context
- Essential for preserving tuple data when building long-lived result sets
- The copied tuple will be automatically freed when the SPI procedure context ends
- Memory context switching ensures the copy is placed in the correct long-term storage area
- Commonly used in stored procedures and functions that need to accumulate or cache tuple data
- The function validates both the input tuple and the SPI connection state before proceeding