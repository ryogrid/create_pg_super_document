# _SPI_procmem

## Location
[src/backend/executor/spi.c:3064-3076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L3064-L3076)

## Overview
 is a utility function that switches to the procedure memory context of the current SPI connection.

## Definition

```c
static MemoryContext
_SPI_procmem(void)
```
## Detailed Description
This function provides a convenient way to switch to the procedure memory context associated with the current SPI connection. It accesses the procCxt field from the current SPI connection structure and switches the active memory context to it using MemoryContextSwitchTo. The function returns the previous memory context, allowing callers to restore it later if needed.

The procedure memory context typically has a longer lifetime than the execution memory context and is used for objects that need to persist across multiple SPI operations within the same procedure call.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md): Core PostgreSQL function that switches active memory context
  - _SPI_current->procCxt: Procedure memory context from current SPI connection
- Called from (representative examples):
  - [spi_dest_startup](../s/spi_dest_startup.md): SPI destination startup operations
  - _SPI_end_call: When cleaning up SPI call context

## Notes and Other Information
- Returns the previous MemoryContext, enabling context restoration
- Provides abstraction over direct memory context switching for SPI operations
- Complements _SPI_execmem by providing access to the procedure-level memory context
- Part of SPI's dual memory context strategy (execution vs procedure contexts)
- Used for allocations that need to survive longer than individual query executions
- Very simple wrapper function that encapsulates SPI-specific memory context access