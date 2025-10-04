# ecpg_store_input

## Location
[src/interfaces/ecpg/ecpglib/execute.c:506-1075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L506-L1075)

## Overview
Converts ECPG variable data into string representations suitable for PostgreSQL SQL statements, handling type conversion, array formatting, null indicators, and memory management for all supported ECPG data types.

## Definition

```c
bool
ecpg_store_input(const int lineno, const bool force_indicator, const struct variable *var,
				 char **tobeinserted_p, bool quote)
```
## Detailed Description
The  function is a comprehensive data conversion utility in ECPG that transforms client-side C variables into their PostgreSQL string representations for use in SQL statements. This function handles a wide range of scenarios:

- **Null Value Handling**: Checks indicator variables to determine if values are NULL
- **Type Conversion**: Supports all ECPG data types including primitives, strings, dates, numerics, and special types
- **Array Processing**: Formats single values and arrays using PostgreSQL array literal syntax ()
- **Memory Management**: Allocates and manages memory for string representations using ECPG's allocation system
- **Quoting and Escaping**: Applies proper PostgreSQL string quoting and escaping where needed
- **Special Value Handling**: Uses specialized formatters for floating-point special values (NaN, Infinity)

The function performs comprehensive type-specific processing, with special handling for complex types like numeric, interval, date, timestamp, bytea, and varchar types.

## Parameters / Member Variables
- `lineno`: Source line number for error reporting and debugging
- `force_indicator`: Boolean flag to force checking of indicator variables for null detection
- `*var`: Pointer to the variable structure containing data type, value, size information, and indicator details
- `**tobeinserted_p`: Output parameter that receives a pointer to the allocated string representation
- `quote`: Boolean flag indicating whether string values should be quoted for SQL context
## Dependencies
- Functions called/Symbols referenced:
  - : Check if non-indicator variable contains null value
  - : ECPG memory allocation with error handling
  - : Format float values with special value handling
  - : Format double values with special value handling
  - : Apply PostgreSQL string quoting and escaping
  - : ECPG string duplication utility
  - : ECPG memory reallocation
  - : ECPG memory deallocation
  - : Raise ECPG errors with proper error codes
  - : Get human-readable type name for errors
  - **PGTYPE functions**: Various PostgreSQL type conversion functions (numeric, date, timestamp, interval)

- Called from (representative examples):
  - : Descriptor-based parameter setting
  - : Input processing from descriptors
  - : SQL parameter building (multiple locations)

## Notes and Other Information
- Supports all major ECPG data types: integers, floats, booleans, strings, dates, timestamps, intervals, numerics, bytea, and varchar
- Implements PostgreSQL array literal format for array variables
- Uses type-specific buffer size calculations to prevent overflow
- Integrates with ECPG's comprehensive error handling system
- Memory allocated by this function becomes the caller's responsibility
- [Boolean](../B/Boolean.md) values are represented as PostgreSQL's 't'/'f' format
- [Complex](../C/Complex.md) types like numeric and dates use PostgreSQL's type library functions for accurate conversion
- Special handling for string types includes proper null termination and quoting
- Returns false on any error condition, with detailed error reporting through ECPG's error system

## Simplified Source

```c
bool
ecpg_store_input(const int lineno, const bool force_indicator, const struct variable *var,
                 char **tobeinserted_p, bool quote)
{
    char *mallocedval = NULL;
    char *newcopy = NULL;

    *tobeinserted_p = "";

    // Check for NULL value via indicator variable
    if (is_null_via_indicator(var, force_indicator)) {
        *tobeinserted_p = NULL;
        return true;
    }

    // Handle non-NULL values based on type
    int asize = var->arrsize ? var->arrsize : 1;

    switch (var->type) {
        case ECPGt_short:
        case ECPGt_int:
        case ECPGt_long:
        case ECPGt_long_long:
        case ECPGt_unsigned_short:
        case ECPGt_unsigned_int:
        case ECPGt_unsigned_long:
        case ECPGt_unsigned_long_long:
            // Format integer types (with array support)
            mallocedval = format_integer_type(var, asize, lineno);
            break;

        case ECPGt_float:
        case ECPGt_double:
            // Format floating-point types with special value handling
            mallocedval = format_float_type(var, asize, lineno);
            break;

        case ECPGt_bool:
            // Format boolean as 't'/'f' with array support
            mallocedval = format_bool_type(var, asize, lineno);
            break;

        case ECPGt_char:
        case ECPGt_unsigned_char:
        case ECPGt_string:
            // Handle string types with proper quoting
            newcopy = prepare_string_data(var, lineno);
            mallocedval = quote_postgres(newcopy, quote, lineno);
            break;

        case ECPGt_varchar:
            // Handle PostgreSQL varchar type
            mallocedval = format_varchar_type(var, quote, lineno);
            break;

        case ECPGt_bytea:
            // Handle binary data
            mallocedval = format_bytea_type(var, lineno);
            break;

        case ECPGt_decimal:
        case ECPGt_numeric:
            // Handle PostgreSQL numeric types
            mallocedval = format_numeric_type(var, asize, lineno);
            break;

        case ECPGt_date:
        case ECPGt_timestamp:
        case ECPGt_interval:
            // Handle date/time types
            mallocedval = format_datetime_type(var, asize, quote, lineno);
            break;

        case ECPGt_descriptor:
        case ECPGt_sqlda:
            // No action needed for descriptor types
            break;

        default:
            // Unsupported type
            ecpg_raise(lineno, ECPG_UNSUPPORTED, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR,
                       ecpg_type_name(var->type));
            return false;
    }

    if (mallocedval) {
        *tobeinserted_p = mallocedval;
    }

    return true;
}
```