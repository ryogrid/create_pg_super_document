# SPI_rollback_and_chain

## Location
[src/backend/executor/spi.c:419-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L419-L427)

## Overview
SPI_rollback_and_chain aborts the current transaction and starts a new one while preserving transaction characteristics, providing chained transaction rollback control for procedural languages and extensions.

## Definition

```c
void
SPI_rollback_and_chain(void)
```
## Detailed Description
SPI_rollback_and_chain provides transaction chaining functionality for rollback operations within the SPI (Server Programming Interface) context. This function is a wrapper around the internal _SPI_rollback function, called with the 'chain' parameter set to true, which means transaction characteristics (such as isolation level, read-only status, and deferrable status) are preserved across the transaction boundary.

Like SPI_rollback, this function performs the same critical validation checks:
1. Verifies that the current SPI context permits transaction termination (not in atomic mode)
2. Ensures no subtransaction is active to maintain subtransaction semantics
3. Protects portals during the transaction boundary

The key difference from SPI_rollback is that transaction characteristics are saved before the rollback and restored in the new transaction, providing continuity of transaction properties across the boundary. This implements SQL standard ROLLBACK AND CHAIN semantics.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_rollback](_SPI_rollback.md) (internal implementation with chain parameter set to true)
- Called from (representative examples):
  - SPI_OPT_NONATOMIC (referenced in header for non-atomic SPI operations)

## Notes and Other Information
- This function can only be called outside of atomic SPI contexts (when SPI_OPT_NONATOMIC is used)
- Cannot be called while a subtransaction is active
- Transaction characteristics (isolation level, read-only status, deferrable status) are preserved across the rollback boundary
- Implements SQL standard ROLLBACK AND CHAIN semantics
- Errors during rollback are handled by attempting to abort again and starting a new transaction with preserved characteristics
- Less commonly used than SPI_rollback, primarily for cases where transaction properties need to be maintained across rollback boundaries
- Useful in scenarios where procedural language code needs to restart a transaction with the same properties after an error
- Provides consistency with the commit and chain functionality offered by SPI_commit_and_chain

## Simplified Source

```c
void SPI_rollback_and_chain(void) {
    // Rollback current transaction and start new one with preserved characteristics
    _SPI_rollback(true);  // true = chain transaction
}
```