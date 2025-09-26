# spi_dest_startup

## Location
[src/backend/executor/spi.c:2123-2170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2123-L2170)

## Overview
Initializes a SPITupleTable to receive tuples from the Executor into the current SPI procedure's context.

## Definition
```c
void spi_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
```

## Detailed Description
spi_dest_startup is a private function that implements the startup phase of the DestReceiver interface for SPI operations. It creates and initializes a SPITupleTable structure that will collect tuples returned by executed queries within the SPI framework. The function establishes proper memory context management by creating a dedicated context for the tuple table and ensuring it's properly registered for cleanup.

The function performs several critical setup tasks: creates a new memory context for tuple storage, allocates and initializes the SPITupleTable structure, sets up initial storage for tuples with a default allocation of 128 slots, and copies the tuple descriptor for type information. It also registers the tuple table with the current SPI context to ensure proper cleanup during subtransaction abort scenarios.

## Parameters / Member Variables
- `self`: Pointer to the DestReceiver structure (used for the destination interface but not directly accessed in this function)
- `operation`: Integer representing the type of operation being performed (not used in this implementation)
- `typeinfo`: TupleDesc containing the structure and type information for tuples that will be received

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_procmem](../S/_SPI_procmem.md) (memory context switching)
  - AllocSetContextCreate (memory context creation)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (transaction tracking)
  - [slist_push_head](slist_push_head.md) (list management)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md) (tuple descriptor copying)
  - [palloc0](../p/palloc0.md), palloc (memory allocation)
  - [SPITupleTable](../S/SPITupleTable.md) (tuple table structure)
  - [DestReceiver](../D/DestReceiver.md) (destination interface)
- Called from (representative examples):
  - Part of the DestReceiver interface (referenced in printtup.h)

## Notes and Other Information
- This is a private function within the SPI implementation
- Validates that SPI is properly connected and no tuple table already exists
- Creates tuple table context as a child of the procedure memory context
- Initial allocation is set to 128 tuples, which can grow as needed
- Registers the tuple table for automatic cleanup during subtransaction abort
- Part of the destination receiver pattern used throughout PostgreSQL's executor
- Essential for SPI query execution that returns result sets