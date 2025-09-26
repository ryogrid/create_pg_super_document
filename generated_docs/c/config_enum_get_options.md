# config_enum_get_options

## Location
src/backend/utils/misc/guc.c: 3074 - 3131

## Overview
Returns a dynamically allocated string containing all available options for an enum GUC parameter, formatted with custom prefix, suffix, and separator.

## Definition


## Detailed Description
This function constructs a formatted string listing all non-hidden options available for a PostgreSQL configuration enum parameter. It iterates through the enum's options array, building a StringInfo buffer that contains each visible option name separated by the specified separator string. The function allows customization through optional prefix and suffix strings.

Hidden enum entries (those with the hidden flag set) are automatically excluded from the output. The function handles edge cases such as when all entries are hidden, ensuring memory safety by checking string length before manipulating the final separator.

This function is commonly used for generating user-friendly error messages and help text that show valid options for enum-type configuration parameters.

## Parameters / Member Variables
- : Pointer to a config_enum structure containing the enum definition and options array
- : Optional string to prepend to the beginning of the options list (can be NULL)
- : Optional string to append to the end of the options list (can be NULL)  
- : String used to separate individual option names in the output

## Dependencies
- Functions called/Symbols referenced:
  - config_enum (struct type)
  - config_enum_entry (struct type)
  - initStringInfo (StringInfo initialization)
  - appendStringInfoString (string appending)
  - appendBinaryStringInfo (binary data appending)
  - strlen (string length calculation)
- Called from (representative examples):
  - parse_and_validate_value
  - GetConfigOptionValues

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Automatically filters out hidden enum options from the output
- Handles edge cases where all enum entries might be hidden
- Uses StringInfo for efficient string building with dynamic memory allocation
- Commonly used in error reporting to show valid enum option values to users
- Part of PostgreSQL's GUC (Grand Unified Configuration) system infrastructure