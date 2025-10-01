# SPI_connect_ext

## Location
[src/backend/executor/spi.c:100-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L100-L181)

## Overview
SPI_connect_ext establishes a connection to PostgreSQL's Server Programming Interface with configurable options, providing the core implementation for SPI connection establishment with atomic or non-atomic execution modes.

## Definition

```c
int
SPI_connect_ext(int options)
```
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
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for initial stack allocation)
  - [repalloc](../r/repalloc.md) (for stack expansion)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (for subtransaction tracking)
  - [slist_init](../s/slist_init.md) (for tuple table list initialization)
  - AllocSetContextCreate (for memory context creation)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (to switch to procedure context)

- Called from (representative examples):
  - [SPI_connect](SPI_connect.md) (wrapper with default options)
  - [plperl_inline_handler](../p/plperl_inline_handler.md), plperl_func_handler (Perl procedural language)
  - [plpython3_call_handler](../p/plpython3_call_handler.md), plpython3_inline_handler (Python procedural language)
  - [pltcl_func_handler](../p/pltcl_func_handler.md) (Tcl procedural language)

## Notes and Other Information
- Must be paired with SPI_finish() to properly clean up the connection and restore previous state
- The SPI stack supports nested connections, allowing recursive SPI usage
- Memory contexts are automatically cleaned up during transaction end via AtEOSubXact_SPI() and AtEOXact_SPI()
- In atomic mode, the connection participates in the current transaction; in non-atomic mode, it can start its own transactions
- Global SPI variables are saved/restored to support nested SPI calls
- Stack corruption detection is performed with assertions and error checks
- Returns SPI_OK_CONNECT (1) on successful connection establishment
- Located in src/backend/executor/spi.c:100-181

## Simplified Source

```c
int
SPI_connect_ext(int options)
{
    int newdepth;

    // Initialize or enlarge the SPI stack if needed
    if (_SPI_stack == NULL)
    {
        // First time initialization
        if (_SPI_connected != -1 || _SPI_stack_depth != 0)
            elog(ERROR, "SPI stack corrupted");

        newdepth = 16;
        _SPI_stack = (SPI_connection *)
            MemoryContextAlloc(TopMemoryContext,
                               newdepth * sizeof(SPI_connection));
        _SPI_stack_depth = newdepth;
    }
    else
    {
        // Check for stack corruption
        if (_SPI_stack_depth <= 0 || _SPI_stack_depth <= _SPI_connected)
            elog(ERROR, "SPI stack corrupted");

        // Double stack size if full
        if (_SPI_stack_depth == _SPI_connected + 1)
        {
            newdepth = _SPI_stack_depth * 2;
            _SPI_stack = (SPI_connection *)
                repalloc(_SPI_stack, newdepth * sizeof(SPI_connection));
            _SPI_stack_depth = newdepth;
        }
    }

    // Enter new stack level and initialize connection state
    _SPI_connected++;
    _SPI_current = &(_SPI_stack[_SPI_connected]);

    // Initialize connection structure
    _SPI_current->processed = 0;
    _SPI_current->tuptable = NULL;
    _SPI_current->execSubid = InvalidSubTransactionId;
    slist_init(&_SPI_current->tuptables);
    _SPI_current->connectSubid = GetCurrentSubTransactionId();
    _SPI_current->queryEnv = NULL;
    _SPI_current->atomic = (options & SPI_OPT_NONATOMIC ? false : true);
    _SPI_current->internal_xact = false;

    // Save outer state for nested SPI calls
    _SPI_current->outer_processed = SPI_processed;
    _SPI_current->outer_tuptable = SPI_tuptable;
    _SPI_current->outer_result = SPI_result;

    // Create memory contexts (atomic vs non-atomic)
    _SPI_current->procCxt = AllocSetContextCreate(
        _SPI_current->atomic ? TopTransactionContext : PortalContext,
        "SPI Proc", ALLOCSET_DEFAULT_SIZES);

    _SPI_current->execCxt = AllocSetContextCreate(
        _SPI_current->atomic ? TopTransactionContext : _SPI_current->procCxt,
        "SPI Exec", ALLOCSET_DEFAULT_SIZES);

    // Switch to procedure context
    _SPI_current->savedcxt = MemoryContextSwitchTo(_SPI_current->procCxt);

    // Reset global SPI variables for new connection
    SPI_processed = 0;
    SPI_tuptable = NULL;
    SPI_result = 0;

    return SPI_OK_CONNECT;
}
```