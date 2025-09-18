# pset_value_string

## Location
src/bin/psql/command.c: 5193 - 5271

## Overview
Returns a formatted string representation of a specific psql print setting parameter value, suitable for display or re-input into the \pset command.

## Definition


## Detailed Description
The pset_value_string function serves as a comprehensive formatter for all psql print settings parameters. It takes a parameter name and a printQueryOpt structure, then returns an appropriately formatted string representation of that parameter's current value. The function handles various data types including integers, booleans, strings, and enums, ensuring that each value is formatted in a way that can be fed back into the \pset command to recreate the same setting.

The function uses a large if-else chain to handle each supported parameter, with special formatting rules for each type. String parameters are quoted and escaped using pset_quoted_string, boolean parameters use pset_bool_string for "on"/"off" representation, and numeric parameters use psprintf for integer formatting. Special cases include the "expanded" parameter which can be "auto", "on", or "off", and the "xheader_width" parameter which supports multiple modes.

## Parameters / Member Variables
- : Name of the parameter to format (must not be NULL)
- : Pointer to the printQueryOpt structure containing current settings

## Dependencies
- Functions called/Symbols referenced:
  - [pset_quoted_string](pset_quoted_string.md) (for string parameter formatting)
  - [pset_bool_string](pset_bool_string.md) (for boolean parameter formatting)
  - [psprintf](psprintf.md) (for integer formatting)
  - [pstrdup](pstrdup.md) (for string duplication)
  - [_align2string](../a/_align2string.md) (for format enum conversion)
  - [get_line_style](../g/get_line_style.md) (for line style information)
  - [_unicode_linestyle2string](../u/_unicode_linestyle2string.md) (for Unicode line style conversion)
  - snprintf (for custom formatting)
- Called from (representative examples):
  - [exec_command_pset](../e/exec_command_pset.md)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Handles NULL parameter values appropriately, often returning empty strings
- The "expanded" parameter has special logic for auto mode (value 2)
- String parameters distinguish between unset (NULL) and empty string cases
- The "xheader_width" parameter supports multiple width types (full, column, page, exact)
- Returns "ERROR" for unrecognized parameter names
- All boolean values use PostgreSQL's "on"/"off" convention
- String values are properly quoted and escaped to handle special characters
- Static function scope limits usage to within command.c
- Essential for implementing the \pset command's parameter display functionality