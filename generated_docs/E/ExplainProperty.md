# ExplainProperty

## Location
src/backend/commands/explain.c: 4749 - 4801

## Overview
Core internal function that formats and outputs simple properties in EXPLAIN output across all supported formats, handling both numeric and text values with optional unit specifications.

## Definition


## Detailed Description
This is the foundational function for displaying simple key-value properties in PostgreSQL EXPLAIN output. It provides format-specific rendering for all supported output modes:

- **TEXT format**: Displays as "label: value unit" with proper indentation and newline
- **XML format**: Creates properly escaped XML tags with the label as the element name and value as content  
- **JSON format**: Creates JSON key-value pairs with proper escaping, treating numeric values as unquoted JSON numbers
- **YAML format**: Creates YAML key-value pairs with proper escaping and numeric handling

The function distinguishes between numeric and text values, ensuring numeric values are not quoted in JSON/YAML formats while text values receive proper escaping. The unit parameter allows appending measurement units in TEXT format.

This function is typically not called directly but serves as the implementation backend for the type-specific wrapper functions like ExplainPropertyText, ExplainPropertyInteger, etc.

## Parameters / Member Variables
- : The property label/name to be displayed
- : Optional unit string (e.g., "ms", "KB") displayed after the value in TEXT format, can be NULL
- : The string representation of the property value
- : Boolean flag indicating if the value should be treated as a number (affects JSON/YAML quoting)
- : Pointer to ExplainState containing output format information and string buffer

## Dependencies
- Functions called/Symbols referenced:
  - ExplainState (struct type)
  - EXPLAIN_FORMAT_TEXT, EXPLAIN_FORMAT_XML, EXPLAIN_FORMAT_JSON, EXPLAIN_FORMAT_YAML (enum values)
  - ExplainIndentText
  - ExplainXMLTag
  - ExplainJSONLineEnding  
  - ExplainYAMLLineStarting
  - appendStringInfo, appendStringInfoString, appendStringInfoChar, appendStringInfoSpaces
  - escape_xml, escape_json, escape_yaml
  - pfree
- Called from (representative examples):
  - ExplainPropertyText (at src/backend/commands/explain.c:4804)
  - ExplainPropertyInteger (at src/backend/commands/explain.c:4817)
  - ExplainPropertyUInteger (at src/backend/commands/explain.c:4830)
  - ExplainPropertyFloat (at src/backend/commands/explain.c:4844)
  - ExplainPropertyBool (at src/backend/commands/explain.c:4854)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Serves as the core implementation for all type-specific ExplainProperty* wrapper functions
- The numeric parameter is crucial for proper JSON/YAML formatting - numeric values appear unquoted while text values are properly escaped and quoted
- Unit specification only affects TEXT format output; other formats ignore the unit parameter
- Memory management includes freeing escaped XML strings
- Proper indentation and line ending handling is maintained across all formats
- The function handles NULL unit parameters gracefully
- XML output uses X_NOWHITESPACE flags to create compact element formatting without extra whitespace