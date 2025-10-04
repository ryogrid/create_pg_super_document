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
  - [PQgetvalue](../P/PQgetvalue.md), PQfformat, PQgetlength, PQgetisnull (libpq result access)
  - [ecpg_hex_encode](ecpg_hex_encode.md), hex_decode (binary data conversion)
  - PGTYPESnumeric_*, PGTYPESinterval_*, PGTYPESdate_*, PGTYPEStimestamp_* (PostgreSQL type conversions)
  - [ecpg_raise](ecpg_raise.md), ecpg_log (error handling and logging)
  - [ECPGset_noind_null](../E/ECPGset_noind_null.md) (NULL value handling for no-indicator mode)
  - [garbage_left](../g/garbage_left.md), check_special_value (parsing utilities)
- Called from (representative examples):
  - [ecpg_store_result](ecpg_store_result.md) (in src/interfaces/ecpg/ecpglib/execute.c:427, 446)
  - [ecpg_set_compat_sqlda](ecpg_set_compat_sqlda.md) (in src/interfaces/ecpg/ecpglib/sqlda.c:399)
  - [ecpg_set_native_sqlda](ecpg_set_native_sqlda.md) (in src/interfaces/ecpg/ecpglib/sqlda.c:584)

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

## Simplified Source

```c
bool ecpg_get_data(const PGresult *results, int act_tuple, int act_field, int lineno,
                   enum ECPGttype type, enum ECPGttype ind_type,
                   char *var, char *ind, long varcharsize, long offset,
                   long ind_offset, enum ARRAY_TYPE isarray, enum COMPAT_MODE compat,
                   bool force_indicator) {

    // Get SQLCA context and extract result data
    struct sqlca_t *sqlca = ECPGget_sqlca();
    if (!sqlca) {
        ecpg_raise(lineno, ECPG_OUT_OF_MEMORY, ECPG_SQLSTATE_ECPG_OUT_OF_MEMORY, NULL);
        return false;
    }

    char *pval = (char *) PQgetvalue(results, act_tuple, act_field);
    int binary = PQfformat(results, act_field);
    int size = PQgetlength(results, act_tuple, act_field);
    int value_for_indicator = 0;

    // Check for NULL value and set indicator
    if (PQgetisnull(results, act_tuple, act_field))
        value_for_indicator = -1;

    // Set indicator variable based on type
    switch (ind_type) {
        case ECPGt_short:
        case ECPGt_unsigned_short:
            *((short *) (ind + ind_offset * act_tuple)) = value_for_indicator;
            break;
        case ECPGt_int:
        case ECPGt_unsigned_int:
            *((int *) (ind + ind_offset * act_tuple)) = value_for_indicator;
            break;
        case ECPGt_NO_INDICATOR:
            if (value_for_indicator == -1) {
                if (!force_indicator) {
                    ECPGset_noind_null(type, var + offset * act_tuple);
                } else {
                    ecpg_raise(lineno, ECPG_MISSING_INDICATOR,
                               ECPG_SQLSTATE_NULL_VALUE_NO_INDICATOR_PARAMETER, NULL);
                    return false;
                }
            }
            break;
        default:
            ecpg_raise(lineno, ECPG_UNSUPPORTED, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR,
                       ecpg_type_name(ind_type));
            return false;
    }

    // Return early if NULL value
    if (value_for_indicator == -1)
        return true;

    // Handle array format validation
    if (isarray == ECPG_ARRAY_ARRAY && *pval != '{') {
        ecpg_raise(lineno, ECPG_DATA_NOT_ARRAY, ECPG_SQLSTATE_DATATYPE_MISMATCH, NULL);
        return false;
    }

    // Main conversion loop (handles arrays and single values)
    do {
        if (binary) {
            // Binary data: direct memory copy with truncation handling
            if (varcharsize == 0 || varcharsize * offset >= size) {
                memcpy(var + offset * act_tuple, pval, size);
            } else {
                memcpy(var + offset * act_tuple, pval, varcharsize * offset);
                if (varcharsize * offset < size) {
                    // Set truncation indicator and warning
                    *((int *) (ind + ind_offset * act_tuple)) = size;
                    sqlca->sqlwarn[0] = sqlca->sqlwarn[1] = 'W';
                }
            }
        } else {
            // Text data: type-specific conversion
            switch (type) {
                case ECPGt_int:
                case ECPGt_long: {
                    long res = strtol(pval, &scan_length, 10);
                    if (garbage_left(isarray, &scan_length, compat)) {
                        ecpg_raise(lineno, ECPG_INT_FORMAT, ECPG_SQLSTATE_DATATYPE_MISMATCH, pval);
                        return false;
                    }
                    *((int *) (var + offset * act_tuple)) = (int) res;
                    pval = scan_length;
                    break;
                }

                case ECPGt_float:
                case ECPGt_double: {
                    double dres;
                    if (!check_special_value(pval, &dres, &scan_length))
                        dres = strtod(pval, &scan_length);

                    if (garbage_left(isarray, &scan_length, ECPG_COMPAT_PGSQL)) {
                        ecpg_raise(lineno, ECPG_FLOAT_FORMAT, ECPG_SQLSTATE_DATATYPE_MISMATCH, pval);
                        return false;
                    }
                    *((double *) (var + offset * act_tuple)) = dres;
                    pval = scan_length;
                    break;
                }

                case ECPGt_string: {
                    char *str = (char *) (var + offset * act_tuple);
                    if (varcharsize > size) {
                        strncpy(str, pval, size + 1);
                        // Right trim spaces for string type
                        char *last = str + size;
                        while (last > str && (*last == ' ' || *last == '\0')) {
                            *last = '\0';
                            last--;
                        }
                    } else {
                        int charsize = varcharsize ? varcharsize : size + 1;
                        strncpy(str, pval, charsize);
                        if (charsize < size) {
                            // Set truncation indicator
                            *((int *) (ind + ind_offset * act_tuple)) = size;
                            sqlca->sqlwarn[0] = sqlca->sqlwarn[1] = 'W';
                        }
                    }
                    pval += size;
                    break;
                }

                case ECPGt_bool:
                    if (pval[0] == 'f' && pval[1] == '\0') {
                        *((bool *) (var + offset * act_tuple)) = false;
                    } else if (pval[0] == 't' && pval[1] == '\0') {
                        *((bool *) (var + offset * act_tuple)) = true;
                    } else {
                        ecpg_raise(lineno, ECPG_CONVERT_BOOL, ECPG_SQLSTATE_DATATYPE_MISMATCH, pval);
                        return false;
                    }
                    pval++;
                    break;

                case ECPGt_bytea: {
                    struct ECPGgeneric_bytea *variable =
                        (struct ECPGgeneric_bytea *) (var + offset * act_tuple);
                    long dst_size = ecpg_hex_enc_len(varcharsize);
                    long src_size = size - 2;  // exclude backslash + 'x'
                    long dec_size = src_size < dst_size ? src_size : dst_size;
                    variable->len = hex_decode(pval + 2, dec_size, variable->arr);
                    pval += size;
                    break;
                }

                default:
                    ecpg_raise(lineno, ECPG_UNSUPPORTED, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR,
                               ecpg_type_name(type));
                    return false;
            }
        }

        // Handle array element advancement
        if (ECPG_IS_ARRAY(isarray)) {
            ++act_tuple;
            // Skip to next array element (simplified delimiter handling)
            while (*pval && !array_delimiter(isarray, *pval) && !array_boundary(isarray, *pval))
                ++pval;
            if (array_delimiter(isarray, *pval))
                ++pval;
        }

    } while (*pval != '\0' && !array_boundary(isarray, *pval));

    return true;
}
```