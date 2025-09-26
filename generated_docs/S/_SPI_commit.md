# _SPI_commit

## Location
[src/backend/executor/spi.c:227-319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L227-L319)

## Overview
_SPI_commit is an internal SPI function that commits the current transaction and optionally chains to a new transaction, providing transaction control for non-atomic SPI connections.

## Definition

```c
static void
_SPI_commit(bool chain)
```
## Detailed Description
_SPI_commit provides the core implementation for transaction commit operations in non-atomic SPI contexts. It performs a complete transaction commit cycle while maintaining proper error handling and ensuring the SPI connection remains valid across transaction boundaries.

The function performs several critical operations:
1. **Validation**: Ensures the SPI connection is in non-atomic mode and not within a subtransaction
2. **Transaction Characteristics**: Optionally saves current transaction characteristics for chaining
3. **Portal Management**: Holds pinned portals and forgets portal snapshots to prevent invalidation
4. **Transaction Commit**: Executes the actual commit using CommitTransactionCommand()
5. **New Transaction**: Immediately starts a new transaction to maintain SPI connection validity
6. **Error Handling**: Comprehensive error recovery that aborts failed transactions and starts fresh

Key restrictions enforced:
- **Atomic Mode Restriction**: Cannot be called in atomic SPI connections (throws ERRCODE_INVALID_TRANSACTION_TERMINATION)
- **Subtransaction Restriction**: Cannot commit while within a subtransaction, as this would violate procedural language exception handling semantics
- **Portal Protection**: Manages portal lifecycle to prevent invalidation during transaction boundaries

The chain parameter controls whether transaction characteristics (isolation level, read-only status, deferrable properties) are preserved across the commit boundary.

## Parameters / Member Variables
- : Boolean flag controlling transaction chaining behavior
  - : Saves and restores transaction characteristics across the commit
  - : Starts new transaction with default characteristics

## Dependencies
- Functions called/Symbols referenced:
  - IsSubTransaction (subtransaction validation)
  - SaveTransactionCharacteristics, RestoreTransactionCharacteristics (transaction state management)
  - HoldPinnedPortals, ForgetPortalSnapshots (portal lifecycle management)
  - CommitTransactionCommand (actual transaction commit)
  - StartTransactionCommand (new transaction initiation)
  - AbortCurrentTransaction (error recovery)
  - CopyErrorData, FlushErrorState, ReThrowError (error handling)

- Called from:
  - SPI_commit (public wrapper without chaining)
  - SPI_commit_and_chain (public wrapper with chaining)

## Notes and Other Information
- **Internal Function**: Static function not exposed in public SPI API
- **Non-Atomic Only**: Can only be used with SPI connections established with SPI_OPT_NONATOMIC
- **Transaction Lifecycle**: Always maintains an active transaction - commits old and starts new
- **Error Recovery**: Robust error handling ensures SPI connection remains valid even after commit failures
- **Portal Management**: Properly handles portal invalidation during transaction boundaries
- **PL Integration**: Designed to support procedural language transaction control requirements
- **Memory Context**: Preserves caller's memory context across transaction boundaries
- **Exception Safety**: Uses PostgreSQL's PG_TRY/PG_CATCH mechanism for exception handling
- Located in src/backend/executor/spi.c:227-319