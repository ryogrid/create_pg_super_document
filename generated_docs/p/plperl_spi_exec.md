# plperl_spi_exec

## Location
[src/pl/plperl/plperl.c:3133-3192](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3133-L3192)

## Overview
Executes an SQL query from within PL/Perl and returns the results as a Perl hash, handling errors through sub-transaction management.

## Definition

```c
HV *
plperl_spi_exec(char *query, int limit)
```
## Detailed Description
This function provides the core SQL execution capability for PL/Perl functions. It wraps the PostgreSQL SPI_execute() call within a sub-transaction to provide proper error handling and cleanup. The function:
1. Creates an internal sub-transaction for safe execution
2. Validates the query string encoding
3. Executes the query using SPI with read-only restrictions based on function properties
4. Converts results to a Perl hash structure
5. Properly commits or rolls back the sub-transaction based on success/failure
6. Converts PostgreSQL errors to Perl exceptions using croak_cstr()

The sub-transaction mechanism ensures that errors don't corrupt the main transaction and provides clean rollback semantics.

## Parameters / Member Variables
- `*query`: The SQL query string to execute
- `limit`: Maximum number of rows to return (0 for unlimited)
## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - PG_TRY/PG_CATCH/PG_END_TRY (exception handling macros)
  - [pg_verifymbstr](pg_verifymbstr.md)
  - [SPI_execute](../S/SPI_execute.md)
  - [plperl_spi_execute_fetch_result](plperl_spi_execute_fetch_result.md)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FlushErrorState](../F/FlushErrorState.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [croak_cstr](../c/croak_cstr.md)
- Global variables referenced:
  - CurrentMemoryContext
  - CurrentResourceOwner
  - SPI_tuptable
  - SPI_processed
  - current_call_data
- Called from (representative examples):
  - Exposed through PL_PERL_H header for use by PL/Perl interface

## Notes and Other Information
- Uses sub-transactions to provide ACID properties and proper error isolation
- Respects the readonly flag from the function descriptor to enforce security
- Validates query encoding to prevent invalid multibyte sequences
- Memory context management ensures proper cleanup even in error cases
- Error messages are converted from PostgreSQL ErrorData to Perl exceptions
- Returns NULL in the error path but this is never reached due to croak_cstr() call
- Forms the foundation for SQL execution in PL/Perl stored procedures and functions