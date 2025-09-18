# buildDefItem

## Location
src/backend/commands/tsearchcmds.c: 1834 - 1870

## Overview
buildDefItem is a static function that constructs a DefElem node from parsed key-value parameters during text search configuration deserialization, with intelligent type detection and conversion.

## Definition


## Detailed Description
This function creates a DefElem (Definition Element) structure from a name-value pair extracted during parameter parsing. It performs intelligent type detection by attempting to parse unquoted values as integers, floats, or booleans before falling back to string representation. Quoted values are always treated as strings to preserve their exact textual representation. This function is essential for reconstructing structured configuration data from serialized text formats.

## Parameters / Member Variables
- : The parameter name/key as a null-terminated string
- : The parameter value as a null-terminated string  
- : Boolean flag indicating whether the original value was enclosed in quotes

## Dependencies
- Functions called/Symbols referenced:
  - strtoint
  - makeDefElem
  - [makeInteger](../m/makeInteger.md)
  - [makeFloat](../m/makeFloat.md)
  - [makeBoolean](../m/makeBoolean.md)
  - [makeString](../m/makeString.md)
  - [pstrdup](../p/pstrdup.md)
  - strtod
  - strcmp
- Called from (representative examples):
  - [ds_state](../d/ds_state.md) (multiple calls during parameter parsing)
  - TSTokenTypeItem

## Notes and Other Information
Located at src/backend/commands/tsearchcmds.c:1834-1870. The function uses a hierarchical type detection approach: first attempting integer parsing with strtoint(), then float parsing with strtod(), then boolean literal matching ("true"/"false"), and finally defaulting to string representation. The was_quoted parameter ensures that explicitly quoted values maintain their string type regardless of content, preserving user intent and preventing unintended type conversions. All string values are duplicated using pstrdup() to ensure proper memory management within PostgreSQL's memory contexts.