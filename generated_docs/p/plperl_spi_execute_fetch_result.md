# plperl_spi_execute_fetch_result

## Location
[src/pl/plperl/plperl.c:3193-3244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3193-L3244)

## Overview
Converts PostgreSQL SPI execution results into a structured Perl hash containing status information and result rows.

## Definition

```c
static HV  *
plperl_spi_execute_fetch_result(SPITupleTable *tuptable, uint64 processed,
								int status)
```
## Detailed Description
This function transforms the raw results from PostgreSQL's SPI execution into a well-structured Perl hash that PL/Perl functions can easily work with. The returned hash contains:
- "status": String representation of the SPI result code
- "processed": Number of rows affected/returned (as appropriate numeric type)
- "rows": Array of hash references, where each hash represents one result row

The function handles large result sets by checking for overflow conditions and uses appropriate Perl data types (UV for integers within range, NV for larger numbers). Each row is converted using plperl_hash_from_tuple() to create nested hash structures.

## Parameters / Member Variables
- `*tuptable`: PostgreSQL SPI tuple table containing the result data
- `processed`: Number of rows processed by the SPI operation
- `status`: SPI result status code indicating success/failure and operation type
## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context)
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [hv_store_string](../h/hv_store_string.md)
  - [SPI_result_code_string](../S/SPI_result_code_string.md)
  - [cstr2sv](../c/cstr2sv.md)
  - newSVnv/newSVuv (Perl scalar value constructors)
  - [plperl_hash_from_tuple](plperl_hash_from_tuple.md)
  - newRV_noinc
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
- Constants referenced:
  - UV_MAX (maximum unsigned value)
  - AV_SIZE_MAX (maximum array size)
- Called from (representative examples):
  - [plperl_spi_exec](plperl_spi_exec.md)
  - [plperl_spi_exec_prepared](plperl_spi_exec_prepared.md)

## Notes and Other Information
- Includes overflow protection for very large result sets that exceed Perl array limits
- Uses appropriate numeric types based on the size of the processed count
- Automatically includes generated columns in result rows (include_generated=true)
- Frees the SPI tuple table to prevent memory leaks
- Returns a structured hash that matches expectations of PL/Perl developers
- Handles both successful queries with data and status-only results (DDL, DML without RETURNING)
- The "rows" field is only populated for successful SELECT-like operations

## Simplified Source

```c
static HV *
plperl_spi_execute_fetch_result(SPITupleTable *tuptable, uint64 processed, int status)
{
    dTHX;
    HV *result;

    check_spi_usage_allowed();

    // Create result hash
    result = newHV();

    // Store status string
    hv_store_string(result, "status", cstr2sv(SPI_result_code_string(status)));

    // Store processed count with appropriate numeric type
    hv_store_string(result, "processed",
                   (processed > (uint64) UV_MAX) ?
                   newSVnv((NV) processed) :
                   newSVuv((UV) processed));

    // Add rows if we have result data
    if (status > 0 && tuptable) {
        AV *rows;
        SV *row;
        uint64 i;

        // Check for overflow in array size
        if (processed > (uint64) AV_SIZE_MAX)
            ereport(ERROR,
                   (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("query result has too many rows to fit in a Perl array")));

        // Create array and convert each tuple to hash
        rows = newAV();
        av_extend(rows, processed);
        for (i = 0; i < processed; i++) {
            row = plperl_hash_from_tuple(tuptable->vals[i], tuptable->tupdesc, true);
            av_push(rows, row);
        }

        hv_store_string(result, "rows", newRV_noinc((SV *) rows));
    }

    // Clean up memory
    SPI_freetuptable(tuptable);

    return result;
}
```