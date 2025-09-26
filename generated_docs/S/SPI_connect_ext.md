# SPI_connect_ext

## Location
src/backend/executor/spi.c: 100 - 181

## Overview
SPI_connect_ext establishes a connection to PostgreSQL's Server Programming Interface with configurable options, providing the core implementation for SPI connection establishment with atomic or non-atomic execution modes.

## Definition


## Detailed Description
SPI_connect_ext is the extended version of SPI_connect that allows callers to specify connection options. It manages the SPI connection stack, handles memory context creation, and initializes all necessary state for SQL command execution within server-side code.

The function performs several critical operations:
1. **Stack Management**: Enlarges the SPI connection stack if necessary (initial size 16, doubles when full)
2. **Connection State**: Increments _SPI_connected to enter a new stack level and initializes a new _SPI_connection structure
3. **Memory Context Creation**: Creates procCxt and execCxt contexts, choosing between TopTransactionContext (atomic) or PortalContext (non-atomic) as parent
4. **State Initialization**: Sets up processed count, tuple table, subtransaction IDs, and execution environment
5. **Global Variable Reset**: Clears SPI_processed, SPI_tuptable, and SPI_result for the new connection

The atomic/non-atomic behavior is controlled by the SPI_OPT_NONATOMIC flag:
- **Atomic mode (default)**: Uses TopTransactionContext, transactions are managed automatically
- **Non-atomic mode**: Uses PortalContext, allows manual transaction control with SPI_commit/SPI_rollback

## Parameters / Member Variables
- : Bitfield controlling connection behavior
  -  (1 << 0): Enables non-atomic mode for manual transaction control
  - : Default atomic mode with automatic transaction management

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAlloc (for initial stack allocation)
  - repalloc (for stack expansion)
  - GetCurrentSubTransactionId (for subtransaction tracking)
  - slist_init (for tuple table list initialization)
  - AllocSetContextCreate (for memory context creation)
  - MemoryContextSwitchTo (to switch to procedure context)

- Called from (representative examples):
  - SPI_connect (wrapper with default options)
  - plperl_inline_handler, plperl_func_handler (Perl procedural language)
  - plpython3_call_handler, plpython3_inline_handler (Python procedural language)
  - pltcl_func_handler (Tcl procedural language)

## Notes and Other Information
- Must be paired with SPI_finish() to properly clean up the connection and restore previous state
- The SPI stack supports nested connections, allowing recursive SPI usage
- Memory contexts are automatically cleaned up during transaction end via AtEOSubXact_SPI() and AtEOXact_SPI()
- In atomic mode, the connection participates in the current transaction; in non-atomic mode, it can start its own transactions
- Global SPI variables are saved/restored to support nested SPI calls
- Stack corruption detection is performed with assertions and error checks
- Returns SPI_OK_CONNECT (1) on successful connection establishment
- Located in src/backend/executor/spi.c:100-181