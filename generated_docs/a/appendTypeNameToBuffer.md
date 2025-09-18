# appendTypeNameToBuffer

## Location
[src/backend/parser/parse_type.c:439-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L439-L477)

## Overview
A static utility function that formats a TypeName structure into a human-readable string representation and appends it to a StringInfo buffer, serving as the core implementation for both TypeNameToString and TypeNameListToString functions.

## Definition


## Detailed Description
This function converts a TypeName structure into its string representation by handling different forms of type specifications. It can process both user-specified type names (with possible schema qualification) and internally-specified types (identified by OID). The function also handles special type decorations like %TYPE for row types and array brackets for array types.

For user-specified names, it concatenates the name components with dots for schema qualification. For internal types, it uses format_type_be() to get the canonical type name. The function adds appropriate decorations based on the TypeName's flags, ensuring a complete and accurate string representation.

This function is designed to work even with TypeNames that don't correspond to actual types, making it useful for error reporting during type lookup failures.

## Parameters / Member Variables
- : The TypeName structure to convert to string format
- : StringInfo buffer to append the formatted type name to

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - lfirst
  - strVal
  - appendStringInfoChar
  - appendStringInfoString
  - [format_type_be](../f/format_type_be.md)
- Called from (representative examples):
  - [TypeNameToString](../T/TypeNameToString.md)
  - [TypeNameListToString](../T/TypeNameListToString.md)

## Notes and Other Information
This is a static function internal to parse_type.c, designed to be the shared implementation for string conversion operations. It handles the complexity of different TypeName formats in a centralized location, ensuring consistent string representation across the codebase. The function carefully handles decorations like %TYPE and array bounds, which are important for accurate type representation in error messages and debugging output. The design allows it to work with invalid or incomplete TypeNames, making it robust for error reporting scenarios.