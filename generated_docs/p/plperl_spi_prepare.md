# plperl_spi_prepare

## Location
src/pl/plperl/plperl.c: 3567 - 3714

## Overview
Prepares a parameterized SQL statement for execution, creating a reusable query plan with type information for efficient repeated execution in PL/Perl functions.

## Definition
```c
SV *plperl_spi_prepare(char *query, int argc, SV **argv)
```

## Detailed Description
This function implements prepared statement functionality for PL/Perl by creating and caching query plans with parameter type information. It provides significant performance benefits for repeatedly executed queries with parameters by avoiding repeated parsing and planning overhead.

Key operations:
1. Creates a dedicated memory context for the query descriptor and related data
2. Parses parameter type names using parseTypeString to resolve type OIDs
3. Prepares type input functions for parameter conversion using getTypeInputInfo
4. Creates the SQL plan using SPI_prepare with parameter types
5. Makes the plan persistent using SPI_keepplan for reuse across calls  
6. Stores the complete query descriptor in a hash table for fast retrieval
7. Returns a unique query identifier that can be used to execute the prepared statement

The function manages complex memory contexts to ensure proper resource lifecycle, using a permanent context for the query descriptor and plan, plus temporary workspace for preparation operations. All operations occur within a subtransaction for safe error handling.

## Parameters / Member Variables
- `query`: C string containing the SQL query with parameter placeholders (e.g., $1, $2, etc.)
- `argc`: Integer count of parameters in the query
- `argv`: Array of SV* pointers containing parameter type names as Perl scalars (e.g., "int4", "text", "timestamp")

## Dependencies
- Functions called/Symbols referenced:
  - check_spi_usage_allowed
  - BeginInternalSubTransaction
  - AllocSetContextCreate
  - sv2cstr
  - parseTypeString
  - getTypeInputInfo
  - fmgr_info_cxt
  - pg_verifymbstr
  - SPI_prepare
  - SPI_result_code_string
  - SPI_keepplan
  - hash_search
  - MemoryContextDelete
  - ReleaseCurrentSubTransaction
  - CopyErrorData
  - FlushErrorState
  - SPI_freeplan
  - RollbackAndReleaseCurrentSubTransaction
  - croak_cstr
  - cstr2sv
- Called from (representative examples):
  - PL_PERL_H header (src/pl/plperl/plperl.h:32)

## Notes and Other Information
- Creates persistent query plans that survive function calls for performance
- Uses hash table storage for fast query plan retrieval by unique identifier
- Comprehensive error handling with automatic cleanup of partially created resources
- Parameter types must be specified as PostgreSQL type names (e.g., "int4", "text")
- Query validation ensures proper encoding before plan creation
- Memory management uses dedicated contexts to prevent leaks
- Plans are made persistent with SPI_keepplan for reuse across transactions
- Returns unique query identifier string for use with execution functions
- Supports complex parameter type resolution including domains and custom types