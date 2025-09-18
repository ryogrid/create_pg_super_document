# plperl_spi_query_prepared

## Location
[src/pl/plperl/plperl.c:3842-3959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3842-L3959)

## Overview
Opens a cursor for a previously prepared SQL statement with parameters in PL/Perl, allowing for streaming result processing rather than fetching all results at once.

## Definition
```c
SV *plperl_spi_query_prepared(char *query, int argc, SV **argv)
```

## Detailed Description
This function creates a cursor for executing a prepared SQL statement in PL/Perl, which is useful for queries that may return large result sets that should be processed incrementally. Unlike plperl_spi_exec_prepared which executes and fetches all results immediately, this function opens a PostgreSQL portal (cursor) and returns a reference to it that can be used for row-by-row processing.

The function follows similar validation and parameter conversion logic as plperl_spi_exec_prepared, but uses SPI_cursor_open instead of SPI_execute_plan. The resulting portal is pinned to prevent it from being automatically closed, and its name is returned as a Perl scalar value. The execution occurs within a sub-transaction for proper error handling.

## Parameters / Member Variables
- `query`: String identifier for the prepared statement to create a cursor for
- `argc`: Number of parameter arguments provided
- `argv`: Array of Perl scalar values to be used as statement parameters

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - [hash_search](../h/hash_search.md) (to find prepared query)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (parameter conversion)
  - [SPI_cursor_open](../S/SPI_cursor_open.md) (create cursor/portal)
  - [SPI_result_code_string](../S/SPI_result_code_string.md) (error reporting)
  - [cstr2sv](../c/cstr2sv.md) (convert portal name to Perl scalar)
  - [PinPortal](../P/PinPortal.md) (prevent automatic portal closure)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md), FlushErrorState (error handling)
  - [croak_cstr](../c/croak_cstr.md) (Perl error propagation)
- Called from (representative examples):
  - PL_PERL_H (header declaration)

## Notes and Other Information
- Returns the portal name as a Perl scalar that can be used with cursor fetch operations
- The portal is pinned to prevent automatic cleanup, requiring explicit closure
- Uses sub-transaction isolation for error safety
- Supports both read-only and read-write cursors based on function properties
- Memory management includes proper cleanup of parameter arrays after portal creation
- Error handling propagates PostgreSQL errors to Perl as exceptions
- The portal remains open across transaction boundaries until explicitly closed