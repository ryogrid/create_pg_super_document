# _outString

## Location
[src/backend/nodes/outfuncs.c:670-683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L670-L683)

## Overview
_outString is a static helper function that serializes a String node to its quoted string representation in PostgreSQL's node output format.

## Definition
```c
static void _outString(StringInfo str, const String *node)
```

## Detailed Description
This function converts a String node into its textual representation by wrapping the string value in double quotes and appending it to the provided StringInfo buffer. The function uses outToken to provide proper escaping of the string content while avoiding the conversion of empty strings to double quotes (since the function explicitly adds quotes around the content). This ensures consistent formatting for string literals in PostgreSQL's abstract syntax tree serialization.

## Parameters / Member Variables
- `str`: StringInfo buffer where the quoted string representation will be appended
- `node`: Pointer to the String node containing the string value (sval field) to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoChar (for adding quote characters)
  - outToken (for escaping string content)
  - String (node type)
- Called from (representative examples):
  - outNode (main node serialization dispatcher)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the outfuncs.c file
- Special handling for empty strings to avoid double-quoting (outToken would normally convert empty strings to "\"\")
- The function manually adds double quotes around the string content
- Part of PostgreSQL's node serialization system used for debugging, logging, and inter-process communication
- The outToken function handles proper escaping of special characters within the string content