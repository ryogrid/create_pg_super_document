# defGetTypeLength

## Location
src/backend/commands/define.c: 312 - 355

## Overview
Extracts a type length indicator from a DefElem, returning either absolute byte length or -1 for variable-length types.

## Definition


## Detailed Description
The  function is a specialized utility for extracting type length specifications in PostgreSQL's type definition system. It handles the parsing of length parameters for data types, supporting both fixed-length types (specified as integer byte counts) and variable-length types (specified using the keyword "variable").

The function processes several input formats:
1. **T_Integer**: Direct integer values representing the byte length
2. **T_Float**: Rejected with an error since type lengths must be integers
3. **T_String**: Accepts the string "variable" (case-insensitive) and returns -1
4. **T_TypeName**: Handles cases where the grammar interprets "variable" as a type name
5. **T_List**: Operator names are not valid for type lengths

The function includes comprehensive error handling and provides detailed error messages for invalid inputs.

## Parameters / Member Variables
- : Pointer to a DefElem structure containing the definition element to extract the type length from

## Dependencies
- Functions called/Symbols referenced:
  - DefElem (structure type)
  - nodeTag (macro to get node type)
  - intVal (macro to extract integer value)
  - pg_strcasecmp (case-insensitive string comparison)
  - strVal (macro to extract string value)
  - TypeNameToString (function to convert TypeName to string)
  - TypeName (structure type)
  - defGetString (function to get string representation)
- Called from (representative examples):
  - DefineType (type definition commands)
  - Functions declared in defrem.h

## Notes and Other Information
- Returns -1 to indicate variable-length types, following PostgreSQL's internal convention
- Provides case-insensitive matching for the "variable" keyword
- Handles grammar ambiguities where "variable" might be parsed as a type name
- Essential for CREATE TYPE statements that specify storage length
- Located in src/backend/commands/define.c:312-355
- Includes fallback return 0 to keep compiler quiet, though unreachable due to error handling