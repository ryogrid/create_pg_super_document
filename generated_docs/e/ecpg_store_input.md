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
- : Source line number for error reporting and debugging
- : Boolean flag to force checking of indicator variables for null detection
- : Pointer to the variable structure containing data type, value, size information, and indicator details
- : Output parameter that receives a pointer to the allocated string representation
- : Boolean flag indicating whether string values should be quoted for SQL context

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
- Boolean values are represented as PostgreSQL's 't'/'f' format
- Complex types like numeric and dates use PostgreSQL's type library functions for accurate conversion
- Special handling for string types includes proper null termination and quoting
- Returns false on any error condition, with detailed error reporting through ECPG's error system