# SPI_commit

## Location
[src/backend/executor/spi.c:320-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L320-L325)

## Overview
SPI_commit commits the current transaction and starts a new one within the SPI (Server Programming Interface) context, providing transaction control for procedural languages and extensions.

## Definition


## Detailed Description
SPI_commit provides a mechanism for procedural languages and SPI-based extensions to commit the current transaction and immediately start a new one. This function is a wrapper around the internal _SPI_commit function, called with the 'chain' parameter set to false, meaning transaction characteristics are not preserved across the commit.

The function performs several critical checks before allowing the commit:
1. Verifies that the current SPI context permits transaction termination (not in atomic mode)
2. Ensures no subtransaction is active, as committing the top-level transaction would violate subtransaction semantics
3. Protects portals during the transaction boundary by holding pinned portals and releasing snapshots

The commit operation is wrapped in a PG_TRY/PG_CATCH block to handle errors gracefully. If the commit fails, the function aborts the failed transaction and starts a new one to maintain a consistent state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_commit (internal implementation with chain parameter set to false)
- Called from (representative examples):
  - [plperl_spi_commit](../p/plperl_spi_commit.md) (from PL/Perl)
  - [PLy_commit](../P/PLy_commit.md) (from PL/Python)
  - pltcl_commit (from PL/Tcl)

## Notes and Other Information
- This function can only be called outside of atomic SPI contexts (when SPI_OPT_NONATOMIC is used)
- Cannot be called while a subtransaction is active
- Transaction characteristics are not preserved across the commit boundary (use SPI_commit_and_chain for that)
- Errors during commit are handled by aborting the failed transaction and starting a new one
- The function is primarily used by procedural language implementations to provide transaction control to user code