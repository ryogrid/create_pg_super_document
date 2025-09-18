# plperl_spi_exec_prepared

## Location
[src/pl/plperl/plperl.c:3715-3841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3715-L3841)

## Overview
Executes a previously prepared SQL statement with parameters in the PL/Perl language extension, handling errors within a sub-transaction to ensure proper cleanup.

## Definition
```c
HV *plperl_spi_exec_prepared(char *query, HV *attr, int argc, SV **argv)
```

## Detailed Description
This function is the core execution mechanism for prepared statements in PL/Perl. It retrieves a previously prepared SQL plan from the query hash, validates parameters, converts Perl scalar values to PostgreSQL Datum values, and executes the plan using SPI (Server Programming Interface). The execution occurs within a sub-transaction to provide proper error isolation - if an error occurs during execution, the sub-transaction is rolled back and the error is propagated to the Perl layer as a croak.

The function performs extensive validation including checking that the prepared query exists, that the number of arguments matches the expected count, and that parameter types are compatible. It supports optional execution attributes such as row limits through the attr parameter.

## Parameters / Member Variables
- `query`: String identifier for the prepared statement to execute
- `attr`: Hash reference containing optional execution attributes (e.g., "limit" for row count limits)  
- `argc`: Number of parameter arguments provided
- `argv`: Array of Perl scalar values to be used as statement parameters

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [BeginInternalSubTransaction](../B/BeginInternalSubTransaction.md)
  - [hash_search](../h/hash_search.md) (to find prepared query)
  - [hv_fetch_string](../h/hv_fetch_string.md) (to parse attributes)
  - [plperl_sv_to_datum](plperl_sv_to_datum.md) (parameter conversion)
  - [SPI_execute_plan](../S/SPI_execute_plan.md) (actual SQL execution)
  - [plperl_spi_execute_fetch_result](plperl_spi_execute_fetch_result.md) (result processing)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [CopyErrorData](../C/CopyErrorData.md), FlushErrorState (error handling)
  - [croak_cstr](../c/croak_cstr.md) (Perl error propagation)
- Called from (representative examples):
  - PL_PERL_H (header declaration)

## Notes and Other Information
- Uses PostgreSQL sub-transaction mechanism for error isolation
- Supports both parameterized and non-parameterized prepared statements
- Converts Perl values to PostgreSQL Datum format using type-specific conversion functions
- Memory management is carefully handled with proper cleanup in error cases
- The function integrates with PostgreSQL's resource management system
- Error messages are propagated to Perl using croak_cstr for proper exception handling
- Supports read-only execution mode based on function properties