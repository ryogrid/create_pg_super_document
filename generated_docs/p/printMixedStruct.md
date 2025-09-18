# printMixedStruct

## Location
src/backend/utils/misc/help_config.c: 87 - 136

## Overview
A formatting function that prints detailed information about a PostgreSQL GUC configuration parameter in a tab-delimited format, handling different parameter types appropriately.

## Definition
```c
static void printMixedStruct(mixedStruct *structToPrint)
```

## Detailed Description
printMixedStruct is responsible for outputting PostgreSQL configuration parameter information in a structured, tab-delimited format. The function first prints common information (parameter name, context, and group), then uses a switch statement to handle type-specific formatting for the parameter value, range, and constraints. It supports five different parameter types: boolean, integer, real/float, string, and enum. The output includes both short and long descriptions of each parameter, making it suitable for generating comprehensive configuration documentation.

## Parameters / Member Variables
- `structToPrint`: Pointer to a mixedStruct containing the configuration parameter to be formatted and printed

## Dependencies
- Functions called/Symbols referenced:
  - printf (standard library)
  - config_enum_lookup_by_value
  - [write_stderr](../w/write_stderr.md)
  - _ (internationalization macro)
- Constants referenced:
  - PGC_BOOL
  - PGC_INT
  - PGC_REAL
  - PGC_STRING
  - PGC_ENUM
- Global arrays referenced:
  - GucContext_Names
  - config_group_names
- Types referenced:
  - mixedStruct
- Called from (representative examples):
  - [GucInfoMain](../G/GucInfoMain.md)

## Notes and Other Information
- Located in src/backend/utils/misc/help_config.c:87-136
- This is a static function, only accessible within the help_config.c file
- Output format is tab-delimited with the following columns:
  1. Parameter name
  2. Context (when the parameter can be changed)
  3. Configuration group
  4. Data type
  5. Default/reset value
  6. Minimum value (for numeric types)
  7. Maximum value (for numeric types)
  8. Short description
  9. Long description
- For boolean parameters: Shows "FALSE" or "TRUE" based on reset_val
- For integer and real parameters: Shows default, minimum, and maximum values
- For string parameters: Shows boot_val (initial value) or empty string if null
- For enum parameters: Uses config_enum_lookup_by_value to convert numeric value to string representation
- Includes error handling for unrecognized parameter types
- Uses internationalization macros (_) for translatable strings