# CopyMultiInsertBufferInit

## Location
[src/backend/commands/copyfrom.c:221-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L221-L237)

## Overview
CopyMultiInsertBufferInit allocates and initializes a new CopyMultiInsertBuffer structure for a specific ResultRelInfo, setting up the buffer for batched tuple insertion during COPY operations.

## Definition


## Detailed Description
This function creates and initializes a CopyMultiInsertBuffer structure that is used to batch multiple tuples before inserting them during COPY FROM operations. The function performs several key initialization steps:

1. **Memory allocation**: Allocates memory for the CopyMultiInsertBuffer structure using palloc
2. **Slot array initialization**: Clears the slots array that will hold TupleTableSlot pointers for batched tuples
3. **ResultRelInfo assignment**: Associates the buffer with the provided ResultRelInfo
4. **BulkInsertState setup**: Initializes bulk insert state for regular tables (not for foreign tables)
5. **Usage counter reset**: Sets the number of used slots to zero

The function specifically handles the distinction between regular tables and foreign tables - for regular tables it obtains a BulkInsertState to optimize buffer allocation, while for foreign tables (identified by non-NULL ri_FdwRoutine) it leaves the bulk insert state as NULL since foreign data wrappers handle their own buffer management.

## Parameters / Member Variables
- : A pointer to ResultRelInfo structure representing the target relation for the COPY operation, containing metadata about the relation and its properties

## Dependencies
- Functions called/Symbols referenced:
  - [CopyMultiInsertBuffer](CopyMultiInsertBuffer.md) (struct type)
  - [palloc](../p/palloc.md) (memory allocation function)
  - memset (memory initialization function)
  - MAX_BUFFERED_TUPLES (constant defining maximum buffered tuples)
  - [GetBulkInsertState](../G/GetBulkInsertState.md) (function to obtain bulk insert state)
- Called from (representative examples):
  - [CopyMultiInsertInfoSetupBuffer](CopyMultiInsertInfoSetupBuffer.md) (at src/backend/commands/copyfrom.c:243)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the copyfrom.c file
- The function differentiates between regular tables and foreign tables for bulk insert state handling
- The slots array is initialized to hold up to MAX_BUFFERED_TUPLES tuple pointers
- Memory is allocated in the current memory context using palloc
- The buffer starts with zero used slots (nused = 0), ready to accept new tuples
- [BulkInsertState](../B/BulkInsertState.md) is only created for regular tables to optimize buffer allocation during bulk operations