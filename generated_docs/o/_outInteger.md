# _outInteger

## Location
[src/backend/nodes/outfuncs.c:648-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L648-L653)

## Overview
Serializes an Integer node to string format by outputting its integer value directly without any additional formatting or type information.

## Definition

```c
static void
_outInteger(StringInfo str, const Integer *node)
```
## Detailed Description
The  function is a simple serialization function for Integer nodes in PostgreSQL's node system. Unlike other node output functions that typically write type information and field names, this function directly appends the integer value to the output string using a straightforward format.

This function is used to serialize Integer nodes, which are part of PostgreSQL's parse tree representation for literal integer values found in SQL queries. The simplicity of this function reflects the straightforward nature of integer literals - they need no complex structural information, just their numeric value.

## Parameters / Member Variables
- : StringInfo buffer where the serialized Integer representation will be written
- : Pointer to the Integer structure containing the integer value to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - appendStringInfo (indirectly through format string expansion)
- Types referenced:
  - Integer
- Called from (representative examples):
  - outNode at src/backend/nodes/outfuncs.c:728

## Notes and Other Information
- This is a static function, used only within the outfuncs.c compilation unit  
- Unlike most other node output functions, this function doesn't use the standard WRITE_NODE_TYPE macro or include type information
- The function directly accesses the  field of the Integer structure to get the numeric value
- Uses standard printf-style formatting ("%d") to convert the integer to its string representation
- Part of PostgreSQL's node serialization system, specifically handling literal integer values from SQL queries
- The output format is minimal and focused purely on the numeric content rather than structural metadata