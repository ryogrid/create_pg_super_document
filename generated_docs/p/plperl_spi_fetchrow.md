# plperl_spi_fetchrow

## Location
src/pl/plperl/plperl.c: 3476 - 3550

## Overview
Fetches the next row from a previously opened SPI cursor, returning the result as a Perl hash reference or undef if no more rows are available.

## Definition
```c
SV *plperl_spi_fetchrow(char *cursor)
```

## Detailed Description
This function implements the row fetching mechanism for SPI cursors in PL/Perl. It takes a cursor name (typically returned from plperl_spi_query) and retrieves the next available row from the result set. The function operates within a subtransaction to ensure proper error handling and resource cleanup.

Key operations:
1. Locates the portal using SPI_cursor_find with the provided cursor name
2. Fetches exactly one row using SPI_cursor_fetch with forward direction
3. Converts the PostgreSQL tuple to a Perl hash using plperl_hash_from_tuple
4. Automatically closes and unpins the cursor when no more rows are available
5. Returns either a hash reference containing column name-value pairs or Perl's undef value

The function ensures proper resource management by unpinning and closing cursors that reach end-of-data, and freeing tuple tables after processing each row.

## Parameters / Member Variables
- `cursor`: C string containing the name of the SPI cursor to fetch from. This should be a cursor name returned from a previous plperl_spi_query call.

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - [SPI_cursor_find](../S/SPI_cursor_find.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - [UnpinPortal](../U/UnpinPortal.md)
  - [SPI_cursor_close](../S/SPI_cursor_close.md)
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md)
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [croak_cstr](../c/croak_cstr.md)
- Called from (representative examples):
  - PL_PERL_H header (src/pl/plperl/plperl.h:31)

## Notes and Other Information
- Uses subtransaction isolation for safe error recovery
- Returns PL_sv_undef when cursor is not found or no more rows are available
- Automatically manages cursor lifecycle - closes cursor at end of data
- Converts PostgreSQL tuples to Perl hash references with column names as keys
- Performs forward-only fetching (one row at a time)
- Memory management includes automatic cleanup of SPI tuple tables
- Uses dTHX macro for thread-safe Perl API access
- Error propagation maintains consistency with Perl exception handling