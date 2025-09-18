# sqlda_common_total_size

## Location
src/interfaces/ecpg/ecpglib/sqlda.c: 65 - 156

## Overview
Calculates the total memory size required to store all field values for a specific row in an SQLDA structure, accounting for proper data type alignment and storage requirements.

## Definition


## Detailed Description
This function computes the memory space needed to store actual data values for all fields in a specific row of a PostgreSQL result set. It iterates through each field, determines the appropriate ECPG data type based on the PostgreSQL type and compatibility mode, then calculates the aligned storage requirements for each value. The function handles a comprehensive range of data types including numeric types, strings, dates, timestamps, decimals, and the complex numeric type which requires additional storage for digit arrays.

Special handling is provided for the numeric type, which requires deconstructing the value to determine the exact size needed for its variable-length digit array. The function ensures all values are properly aligned according to their type requirements for optimal memory access and compatibility across different architectures.

## Parameters / Member Variables
- : Pointer to PostgreSQL result set containing the data and metadata
- : Zero-based row index in the result set to calculate storage for
- : Compatibility mode (ECPG_COMPAT or ECPG_INFORMIX) affecting type mapping
- : Starting memory offset where data storage calculations should begin

## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md) (get number of fields)
  - [sqlda_dynamic_type](sqlda_dynamic_type.md) (map PostgreSQL types to ECPG types)
  - PQftype (get field type)
  - [ecpg_sqlda_align_add_size](../e/ecpg_sqlda_align_add_size.md) (alignment calculations)
  - [PQgetisnull](../P/PQgetisnull.md) (check for NULL values)
  - [PQgetvalue](../P/PQgetvalue.md) (get field value as string)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md) (parse numeric values)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md) (free numeric resources)
  - Various ECPG type constants (ECPGt_short, ECPGt_int, etc.)
- Called from (representative examples):
  - [sqlda_compat_total_size](sqlda_compat_total_size.md)
  - [sqlda_native_total_size](sqlda_native_total_size.md)

## Notes and Other Information
This function is central to ECPG's SQLDA memory management, ensuring that sufficient space is allocated for data storage while maintaining proper alignment. The comprehensive type handling covers all standard SQL data types and their C equivalents. The numeric type handling is particularly complex due to its variable-length nature. The function is used by both compatibility and native SQLDA implementations, making it a shared utility for different SQLDA modes. Memory alignment is critical for performance and correctness, especially on architectures with strict alignment requirements.