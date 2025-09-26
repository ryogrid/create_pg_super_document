# ecpg_set_native_sqlda

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:444-592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L444-L592)

## Overview
Sets up and populates a native SQLDA structure with data values from a specific row of a PostgreSQL result set, handling proper data type conversion and memory layout for the native format.

## Definition

```c
void
ecpg_set_native_sqlda(int lineno, struct sqlda_struct **_sqlda, const PGresult *res, int row, enum COMPAT_MODE compat)
```
## Detailed Description
This function populates a pre-allocated native SQLDA structure with actual data values from a specified row in a PostgreSQL query result. Similar to ecpg_set_compat_sqlda, it handles the complex task of setting up data pointers within the SQLDA structure, performing proper memory alignment, and converting PostgreSQL result data into appropriate C data types based on the SQLDA field types.

The function processes each field in the native SQLDA, calculates proper memory offsets with alignment, sets up sqldata pointers to point to the correct locations within the SQLDA buffer, and converts PostgreSQL result data to the target data type. It handles the same data types as the compatibility version but uses the native sqlda_struct format instead of sqlda_compat. The main difference from the compatibility version is the absence of sqlilongdata handling for long strings and some differences in the structure layout.

## Parameters / Member Variables
- : Line number for logging and debugging purposes
- : Double pointer to the sqlda_struct (native format) to be populated
- : PGresult structure containing the query results
- : Row number in the result set to extract data from (negative values cause early return)
- : Compatibility mode that affects data type handling

## Dependencies
- Functions called/Symbols referenced:
  - [sqlda_native_empty_size](../s/sqlda_native_empty_size.md)
  - [ecpg_sqlda_align_add_size](ecpg_sqlda_align_add_size.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PGTYPESnumeric_from_asc](../P/PGTYPESnumeric_from_asc.md)
  - [PGTYPESnumeric_free](../P/PGTYPESnumeric_free.md)
  - [ECPGset_noind_null](../E/ECPGset_noind_null.md)
  - [ecpg_get_data](ecpg_get_data.md)
  - [ecpg_log](ecpg_log.md)
- Called from (representative examples):
  - [ecpg_process_output](ecpg_process_output.md)

## Notes and Other Information
- This function works with the native SQLDA format (sqlda_struct) rather than the compatibility format (sqlda_compat)
- Does not allocate the SQLDA structure itself; only populates an existing one
- Handles the same wide variety of PostgreSQL data types as the compatibility version
- Special handling for numeric types includes copying digit buffers and adjusting internal pointers
- Unlike the compatibility version, does not handle sqlilongdata for very long strings
- Uses proper memory alignment to ensure efficient data access on different architectures
- Sets up NULL indicators for each field using predefined global values
- Part of the ECPG embedded SQL interface for PostgreSQL client applications providing native SQLDA support