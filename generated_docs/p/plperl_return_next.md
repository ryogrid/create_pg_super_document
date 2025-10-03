# plperl_return_next

## Location
[src/pl/plperl/plperl.c:3245-3274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3245-L3274)

## Overview
Safely handles the return_next operation for PL/Perl set-returning functions, converting PostgreSQL errors to Perl exceptions.

## Definition

```c
void
plperl_return_next(SV *sv)
```
## Detailed Description
This function provides a safe wrapper around the internal return_next functionality for PL/Perl set-returning functions. It implements PostgreSQL's standard error handling pattern using PG_TRY/PG_CATCH blocks to intercept any PostgreSQL errors that occur during the return_next operation and convert them to Perl exceptions using croak_cstr().

The function is designed to be called from PL/Perl code when a set-returning function needs to yield individual result values. It delegates the actual work to plperl_return_next_internal() while providing proper error boundary management.

The error handling assumes that Perl code may trap the converted error, so it doesn't abort the current transaction, allowing for more flexible error recovery in PL/Perl functions.

## Parameters / Member Variables
- `*sv`: Perl scalar value (SV*) to be returned as the next element in the result set
## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling macros)
  - [plperl_return_next_internal](plperl_return_next_internal.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [croak_cstr](../c/croak_cstr.md)
- Global variables referenced:
  - CurrentMemoryContext
- Called from (representative examples):
  - Exposed through PL_PERL_H header for use by PL/Perl interface

## Notes and Other Information
- Part of the set-returning function (SRF) infrastructure in PL/Perl
- Provides error boundary between PostgreSQL and Perl execution contexts
- Assumes Perl error trapping doesn't require transaction abort (design decision with acknowledged uncertainty)
- Memory context management ensures proper cleanup during error handling
- The actual implementation logic is delegated to plperl_return_next_internal()
- Essential for PL/Perl functions that return multiple rows or values incrementally
- Error conversion allows Perl exception handling mechanisms to work with PostgreSQL errors