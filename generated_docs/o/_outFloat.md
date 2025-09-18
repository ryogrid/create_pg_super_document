# _outFloat

## Location
[src/backend/nodes/outfuncs.c:654-663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L654-L663)

## Overview
Serializes a Float node to string format by outputting its string representation directly, assuming the value is already a valid numeric literal.

## Definition


## Detailed Description
The  function serializes Float nodes in PostgreSQL's node system by directly appending the string representation of the floating-point value to the output buffer. The function is designed with the assumption that the float value stored in the node is already in a valid numeric literal format that doesn't require additional quoting or escaping.

This straightforward approach reflects PostgreSQL's design where Float nodes store their values as string representations rather than native floating-point types, preserving the exact textual representation from the original SQL query. This prevents potential precision loss or formatting changes that could occur with binary floating-point conversion.

## Parameters / Member Variables
- : StringInfo buffer where the serialized Float representation will be written
- : Pointer to the Float structure containing the string representation of the floating-point value

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfoString
- Types referenced:
  - Float
- Called from (representative examples):
  - outNode at src/backend/nodes/outfuncs.c:730

## Notes and Other Information
- This is a static function, used only within the outfuncs.c compilation unit
- Similar to , this function doesn't use standard WRITE_NODE_TYPE macros or include type information
- The function accesses the  field of the Float structure, which stores the value as a string rather than a native float/double
- The comment explicitly states the assumption that the value is a valid numeric literal, eliminating the need for quoting
- This string-based approach preserves the exact representation from the original SQL query text
- Part of PostgreSQL's node serialization system for handling literal floating-point values from SQL queries
- The design choice to store floats as strings helps maintain precision and original formatting