# SPI_commit_and_chain

## Location
[src/backend/executor/spi.c:326-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L326-L331)

## Overview
SPI_commit_and_chain commits the current transaction and starts a new one while preserving transaction characteristics, providing chained transaction control for procedural languages and extensions.

## Definition


## Detailed Description
SPI_commit_and_chain provides transaction chaining functionality within the SPI (Server Programming Interface) context. This function is a wrapper around the internal _SPI_commit function, called with the 'chain' parameter set to true, which means transaction characteristics (such as isolation level, read-only status, and deferrable status) are preserved across the transaction boundary.

Like SPI_commit, this function performs the same critical validation checks:
1. Verifies that the current SPI context permits transaction termination (not in atomic mode)
2. Ensures no subtransaction is active to maintain subtransaction semantics
3. Protects portals during the transaction boundary

The key difference from SPI_commit is that transaction characteristics are saved before the commit and restored in the new transaction, providing continuity of transaction properties across the boundary. This implements SQL standard COMMIT AND CHAIN semantics.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_commit (internal implementation with chain parameter set to true)
- Called from (representative examples):
  - SPI_OPT_NONATOMIC (referenced in header for non-atomic SPI operations)

## Notes and Other Information
- This function can only be called outside of atomic SPI contexts (when SPI_OPT_NONATOMIC is used)
- Cannot be called while a subtransaction is active
- Transaction characteristics (isolation level, read-only status, deferrable status) are preserved across the commit boundary
- Implements SQL standard COMMIT AND CHAIN semantics
- Errors during commit are handled by aborting the failed transaction and starting a new one with preserved characteristics
- Less commonly used than SPI_commit, primarily for cases where transaction properties need to be maintained across boundaries