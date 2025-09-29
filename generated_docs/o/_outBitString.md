# _outBitString

## Location
[src/backend/nodes/outfuncs.c:684-695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L684-L695)

## Overview
_outBitString is a static helper function that serializes a BitString node to its string representation in PostgreSQL's node output format.

## Definition
```c
static void _outBitString(StringInfo str, const BitString *node)
```

## Detailed Description
This function converts a BitString node into its textual representation by outputting the bit string value using outToken for proper escaping. The function relies on the lexer guarantee that bit string values always start with 'b' (for binary) or 'x' (for hexadecimal), and outToken preserves these prefixes while escaping any characters that follow. This design ensures that nodeTokenType can correctly identify the token type based on the preserved prefix.

## Parameters / Member Variables
- `str`: StringInfo buffer where the bit string representation will be appended
- `node`: Pointer to the BitString node containing the bit string value (bsval field) to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for validating the bit string format)
  - [outToken](outToken.md) (for escaping and outputting the bit string value)
  - [BitString](../B/BitString.md) (node type)
- Called from (representative examples):
  - [outNode](outNode.md) (main node serialization dispatcher)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the outfuncs.c file
- Contains an assertion that validates the bit string format (must start with 'b' or 'x')
- The lexer guarantees the proper format, but the assertion provides runtime validation
- The outToken function handles escaping while preserving the 'b'/'x' prefix as required by nodeTokenType
- Part of PostgreSQL's node serialization system used for debugging, logging, and inter-process communication
- Bit strings represent binary or hexadecimal literal values in SQL queries

## Simplified Source

```c
static void
_outBitString(StringInfo str, const BitString *node)
{
    // Bit strings always start with 'b' (binary) or 'x' (hex)
    // This is guaranteed by the lexer and required by nodeTokenType
    Assert(node->bsval[0] == 'b' || node->bsval[0] == 'x');

    // Output the bit string value, preserving the b/x prefix
    outToken(str, node->bsval);
}
```