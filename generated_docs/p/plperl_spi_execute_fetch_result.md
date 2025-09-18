# plperl_spi_execute_fetch_result

## Location
src/pl/plperl/plperl.c: 3193 - 3244

## Overview
Converts PostgreSQL SPI execution results into a structured Perl hash containing status information and result rows.

## Definition


## Detailed Description
This function transforms the raw results from PostgreSQL's SPI execution into a well-structured Perl hash that PL/Perl functions can easily work with. The returned hash contains:
- "status": String representation of the SPI result code
- "processed": Number of rows affected/returned (as appropriate numeric type)
- "rows": Array of hash references, where each hash represents one result row

The function handles large result sets by checking for overflow conditions and uses appropriate Perl data types (UV for integers within range, NV for larger numbers). Each row is converted using plperl_hash_from_tuple() to create nested hash structures.

## Parameters / Member Variables
- : PostgreSQL SPI tuple table containing the result data
- : Number of rows processed by the SPI operation
- : SPI result status code indicating success/failure and operation type

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context)
  - check_spi_usage_allowed
  - hv_store_string
  - SPI_result_code_string
  - cstr2sv
  - newSVnv/newSVuv (Perl scalar value constructors)
  - plperl_hash_from_tuple
  - newRV_noinc
  - SPI_freetuptable
- Constants referenced:
  - UV_MAX (maximum unsigned value)
  - AV_SIZE_MAX (maximum array size)
- Called from (representative examples):
  - plperl_spi_exec
  - plperl_spi_exec_prepared

## Notes and Other Information
- Includes overflow protection for very large result sets that exceed Perl array limits
- Uses appropriate numeric types based on the size of the processed count
- Automatically includes generated columns in result rows (include_generated=true)
- Frees the SPI tuple table to prevent memory leaks
- Returns a structured hash that matches expectations of PL/Perl developers
- Handles both successful queries with data and status-only results (DDL, DML without RETURNING)
- The "rows" field is only populated for successful SELECT-like operations