# SPI_rollback

## Location
[src/backend/executor/spi.c:413-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L413-L418)

## Overview
SPI_rollback aborts the current transaction and starts a new one within the SPI (Server Programming Interface) context, providing transaction rollback control for procedural languages and extensions.

## Definition

```c
void
SPI_rollback(void)
```
## Detailed Description
SPI_rollback provides a mechanism for procedural languages and SPI-based extensions to abort the current transaction and immediately start a new one. This function is a wrapper around the internal _SPI_rollback function, called with the 'chain' parameter set to false, meaning transaction characteristics are not preserved across the rollback.

The function performs the same critical validation checks as the commit functions:
1. Verifies that the current SPI context permits transaction termination (not in atomic mode)
2. Ensures no subtransaction is active, as rolling back the top-level transaction would violate subtransaction semantics
3. Protects portals during the transaction boundary by holding pinned portals and releasing snapshots

The rollback operation is wrapped in a PG_TRY/PG_CATCH block to handle errors gracefully. If the rollback fails, the function attempts to abort again and starts a new transaction to maintain a consistent state.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [_SPI_rollback](_SPI_rollback.md) (internal implementation with chain parameter set to false)
- Called from (representative examples):
  - [plperl_spi_rollback](../p/plperl_spi_rollback.md) (from PL/Perl)
  - [PLy_rollback](../P/PLy_rollback.md) (from PL/Python)
  - [pltcl_rollback](../p/pltcl_rollback.md) (from PL/Tcl)

## Notes and Other Information
- This function can only be called outside of atomic SPI contexts (when SPI_OPT_NONATOMIC is used)
- Cannot be called while a subtransaction is active
- Transaction characteristics are not preserved across the rollback boundary (use SPI_rollback_and_chain for that)
- Errors during rollback are handled by attempting to abort again and starting a new transaction
- The function is primarily used by procedural language implementations to provide transaction control to user code
- Unlike commit operations, rollback operations are generally more forgiving of error conditions
- Commonly used in exception handling blocks within procedural languages

## Simplified Source

```c
void SPI_rollback(void) {
    // Rollback current transaction without preserving characteristics
    _SPI_rollback(false);  // false = no chaining
}
```