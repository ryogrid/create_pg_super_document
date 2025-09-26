ecpg_get_data

## Overview
Core ECPG data extraction function that converts PostgreSQL result values into C host variables with comprehensive type handling, indicator variable support, and array processing capabilities.

## Definition
```c
bool ecpg_get_data(const PGresult *results, int act_tuple, int act_field, int lineno,
                   enum ECPGttype type, enum ECPGttype ind_type,
                   char *var, char *ind, long varcharsize, long offset,
                   long ind_offset, enum ARRAY_TYPE isarray, enum COMPAT_MODE compat, 
                   bool force_indicator)
```

## Detailed Description
This function serves as the central data conversion engine for ECPG (Embedded SQL in C), responsible for extracting data from PostgreSQL query results and converting it to appropriate C data types. The function handles a comprehensive range of PostgreSQL data types including integers, floats, booleans, strings, bytea, numeric, date/time types, and arrays.

The function performs several critical operations:
1. **NULL handling**: Detects NULL values and sets appropriate indicator variables
2. **Type conversion**: Converts PostgreSQL string/binary data to target C types
3. **Array processing**: Handles PostgreSQL array syntax and converts to C array elements
4. **Truncation detection**: Monitors data truncation and sets warning indicators
5. **Compatibility modes**: Supports different SQL compatibility modes (PostgreSQL, Oracle, Informix)
6. **Error handling**: Provides detailed error reporting with line number information

The conversion process varies based on whether data is in binary or text format, with extensive validation and error checking throughout. Special handling is provided for complex types like numeric/decimal, intervals, dates, timestamps, and binary data (bytea).

## Parameters / Member Variables
- `results`: PostgreSQL result set containing the data to extract
- `act_tuple`: Current tuple (row) index within the result set
- `act_field`: Current field (column) index within the tuple
- `lineno`: Source code line number for error reporting
- `type`: Target C data type for the conversion (ECPGt_int, ECPGt_string, etc.)
- `ind_type`: Data type of the indicator variable (ECPGt_short, ECPGt_int, etc.)
- `var`: Pointer to the target C variable where data will be stored
- `ind`: Pointer to the indicator variable for NULL/truncation status
- `varcharsize`: Maximum size for character data types (0 for unlimited)
- `offset`: Byte offset for array element positioning
- `ind_offset`: Byte offset for indicator array element positioning
- `isarray`: Array processing mode (ECPG_ARRAY_ARRAY, ECPG_ARRAY_VECTOR, etc.)
- `compat`: SQL compatibility mode (PostgreSQL, Oracle, Informix)
- `force_indicator`: Whether to require indicator variables for NULL values

## Dependencies
- Functions called/Symbols referenced:
  - ECPGget_sqlca (SQLCA context retrieval)
  - PQgetvalue, PQfformat, PQgetlength, PQgetisnull (libpq result access)
  - ecpg_hex_encode, hex_decode (binary data conversion)
  - PGTYPESnumeric_*, PGTYPESinterval_*, PGTYPESdate_*, PGTYPEStimestamp_* (PostgreSQL type conversions)
  - ecpg_raise, ecpg_log (error handling and logging)
  - ECPGset_noind_null (NULL value handling for no-indicator mode)
  - garbage_left, check_special_value (parsing utilities)
- Called from (representative examples):
  - ecpg_store_result (in src/interfaces/ecpg/ecpglib/execute.c:427, 446)
  - ecpg_set_compat_sqlda (in src/interfaces/ecpg/ecpglib/sqlda.c:399)
  - ecpg_set_native_sqlda (in src/interfaces/ecpg/ecpglib/sqlda.c:584)

## Notes and Other Information
- Returns true on successful conversion, false on error
- Handles both single values and array elements through the same interface
- Provides comprehensive truncation warnings through SQLCA warning indicators
- Supports regression test mode where offset logging is suppressed for reproducible output
- Implements different NULL handling strategies based on compatibility mode
- Performs extensive input validation and provides detailed error messages
- The function is quite large (768 lines) due to comprehensive type coverage and error handling
- Memory management is handled carefully with proper cleanup for dynamically allocated types
- Binary vs text format detection is handled automatically based on PQfformat results