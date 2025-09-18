# plperl_spi_commit

## Location
src/pl/plperl/plperl.c: 3991 - 4016

## Overview
Commits the current transaction from within a PL/Perl function, with proper error handling to propagate PostgreSQL commit failures to Perl.

## Definition
```c
void plperl_spi_commit(void)
```

## Detailed Description
This function provides a PL/Perl interface to PostgreSQL's SPI_commit() function, allowing Perl code to explicitly commit the current transaction. It wraps the SPI call in a PG_TRY/PG_CATCH block to handle any errors that might occur during the commit process, such as constraint violations or other database errors that prevent the transaction from being successfully committed.

The function is designed to integrate seamlessly with Perl's exception handling mechanism by converting PostgreSQL errors into Perl exceptions (croak). This ensures that commit failures are properly propagated up the call stack and can be handled using standard Perl error handling techniques.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - check_spi_usage_allowed
  - SPI_commit (actual transaction commit)
  - CopyErrorData, FlushErrorState (error handling)
  - croak_cstr (Perl error propagation)
- Called from (representative examples):
  - PL_PERL_H (header declaration)

## Notes and Other Information
- Only available in functions that allow SPI usage (checked by check_spi_usage_allowed)
- Converts PostgreSQL commit errors into Perl exceptions for consistent error handling
- Memory context is preserved during error handling to prevent resource leaks
- This function enables explicit transaction control from PL/Perl, supporting procedural code that needs to commit work incrementally
- Should be used carefully in conjunction with proper error handling in the calling Perl code
- Part of the transaction control API that also includes plperl_spi_rollback