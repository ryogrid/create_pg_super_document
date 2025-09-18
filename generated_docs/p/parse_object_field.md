# parse_object_field

## Location
src/common/jsonapi.c: 1052 - 1113

## Overview
A recursive descent parsing function that processes JSON object field definitions consisting of a field name (key), colon separator, and field value.

## Definition


## Detailed Description
parse_object_field handles the parsing of individual JSON object fields following the pattern "fieldname" : value. It validates that the field name is a string token, extracts and preserves the field name for semantic callbacks, processes the colon separator, and then recursively parses the field value which can be a scalar, object, or array. The function manages semantic action callbacks for both field start and field end events, passing the field name and null status information. It supports the complete JSON value grammar for field values and ensures proper syntax validation throughout the parsing process.

## Parameters / Member Variables
- : JsonLexContext pointer containing the current parsing state positioned at a field name token
- : JsonSemAction pointer containing object field start/end callback functions and semantic state

## Dependencies
- Functions called/Symbols referenced:
  - lex_peek (for token lookahead and validation)
  - report_parse_error (for syntax error reporting)
  - json_lex (for token consumption)
  - lex_expect (for colon separator validation)
  - parse_object (for nested object values)
  - parse_array (for array values)
  - parse_scalar (for scalar values)
  - pstrdup (for field name string duplication)
- Called from (representative examples):
  - parse_object (src/common/jsonapi.c:1153, 1159) - when processing object field lists

## Notes and Other Information
The function follows JSON object field syntax strictly, requiring string field names followed by colon separators. Field names are extracted and preserved for callback functions when object field semantic actions are provided. The function determines null status by checking if the field value is a JSON_TOKEN_NULL before invoking callbacks. It recursively handles complex field values by delegating to appropriate parsing functions based on the value's leading token. The semantic callbacks receive consistent field name and null status information for both start and end events.