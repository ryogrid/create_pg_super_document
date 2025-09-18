# ecpg_set_compat_sqlda

## Location
src/interfaces/ecpg/ecpglib/sqlda.c: 255 - 411

## Overview
Sets up and populates a compatibility-mode SQLDA structure with data values from a specific row of a PostgreSQL result set, including proper data type conversion and memory layout.

## Definition


## Detailed Description
This function populates a pre-allocated compatibility SQLDA structure with actual data values from a specified row in a PostgreSQL query result. It handles the complex task of setting up data pointers within the SQLDA structure, performing proper memory alignment, and converting PostgreSQL result data into the appropriate C data types based on the SQLDA field types.

The function processes each field in the SQLDA, calculates proper memory offsets with alignment, sets up sqldata pointers to point to the correct locations within the SQLDA buffer, and converts the PostgreSQL result data to the target data type. It handles special cases like numeric types that require additional buffer space for digit storage, string data with length considerations, and NULL value indicators.

## Parameters / Member Variables
- : Line number for logging and debugging purposes
- : Double pointer to the sqlda_compat structure to be populated
- : PGresult structure containing the query results
- : Row number in the result set to extract data from (negative values cause early return)
- : Compatibility mode that affects data type handling

## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_compat_empty_size](../s/sqlda_compat_empty_size.md)
  - [ecpg_sqlda_align_add_size](ecpg_sqlda_align_add_size.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - [ECPGset_noind_null](../E/ECPGset_noind_null.md)
  - ecpg_get_data
  - [ecpg_log](ecpg_log.md)
- Called from (representative examples):
  - ecpg_process_output

## Notes and Other Information
- This function does not allocate the SQLDA structure itself; it only populates an existing one
- Handles a wide variety of PostgreSQL data types including numeric, date/time, and string types
- Special handling for numeric types includes copying digit buffers and adjusting internal pointers
- Sets up NULL indicators for each field using predefined global values
- String data longer than 32768 bytes gets special handling with sqlilongdata pointer
- Uses proper memory alignment to ensure data can be accessed efficiently on different architectures
- Part of the ECPG embedded SQL interface for PostgreSQL client applications