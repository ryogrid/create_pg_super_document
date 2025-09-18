# plperl_spi_query

## Location
src/pl/plperl/plperl.c: 3404 - 3475

## Overview
Executes a SQL query string through SPI interface, returning a cursor name that can be used to fetch results in PL/Perl functions.

## Definition
```c
SV *plperl_spi_query(char *query)
```

## Detailed Description
This function provides PL/Perl functions with the ability to execute arbitrary SQL queries through PostgreSQL's Server Programming Interface (SPI). It creates a prepared statement and cursor for the given query, executing the operation within a subtransaction to handle errors gracefully.

Key features:
- Validates query encoding using pg_verifymbstr
- Creates a prepared plan using SPI_prepare 
- Opens a cursor for the plan using SPI_cursor_open
- Pins the portal to prevent premature cleanup
- Wraps execution in a subtransaction for proper error handling
- Returns the cursor name as a Perl scalar value (SV*)

The function handles errors by rolling back the subtransaction and propagating the error message to Perl using croak_cstr, maintaining clean separation between PostgreSQL and Perl error handling.

## Parameters / Member Variables
- `query`: C string containing the SQL query to execute. Must be validly encoded and null-terminated.

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - [pg_verifymbstr](pg_verifymbstr.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [SPI_freeplan](../S/SPI_freeplan.md)
  - [cstr2sv](../c/cstr2sv.md)
  - [PinPortal](../P/PinPortal.md)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [croak_cstr](../c/croak_cstr.md)
- Called from (representative examples):
  - PL_PERL_H header (src/pl/plperl/plperl.h:30)

## Notes and Other Information
- Uses subtransaction isolation to ensure clean error recovery
- The returned cursor name can be used with other SPI cursor functions
- [Portal](../P/Portal.md) is pinned to prevent garbage collection until explicitly closed
- [Query](../Q/Query.md) parameter validation ensures proper encoding before execution
- Memory context management preserves function-level allocations
- Error propagation uses Perl's croak mechanism for consistent exception handling
- No query parameters are supported (uses 0 parameters in SPI_prepare)