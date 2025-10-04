# outChar

## Location
[src/backend/nodes/outfuncs.c:190-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/outfuncs.c#L190-L210)

## Overview
Converts a single character into a safely escaped string format for PostgreSQL node serialization, delegating to outToken for proper character escaping.

## Definition
static void outChar(StringInfo str, char c)

## Detailed Description
The outChar function is a specialized wrapper around outToken that handles the serialization of individual characters in PostgreSQL's node output system. It serves as a bridge between single character values and the string-based token output mechanism.

The function has special handling for the null character ('\\0') which is traditionally represented as '<>' in PostgreSQL's serialization format for historical consistency. For all other characters, it creates a temporary two-character string (the character plus null terminator) and delegates to outToken for proper escaping and formatting.

This approach ensures that single characters receive the same escaping treatment as longer strings, maintaining consistency in the serialization format and ensuring that special characters that have meaning in the parser are properly protected with backslashes.

## Parameters / Member Variables
- `str`: StringInfo buffer where the escaped character will be appended
- `c`: Single character to be converted and escaped

## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md) (for null character '<>' representation)
  - [outToken](outToken.md) (for delegating character escaping and formatting)

- Called from (representative examples):
  - WRITE_CHAR_FIELD (macro in outfuncs.c:70)

## Notes and Other Information
- This function is declared static, meaning it's only used within the outfuncs.c file
- The null character handling maintains historical compatibility with PostgreSQL's serialization format
- By delegating to outToken, this function automatically benefits from all the special character escaping rules
- The temporary string creation is efficient as it only requires a 2-byte stack allocation
- Part of the broader node serialization infrastructure that ensures round-trip fidelity for all data types

## Simplified Source

```c
static void
outChar(StringInfo str, char c)
{
    // Special case: represent null character as <>
    if (c == '\0')
    {
        appendStringInfoString(str, "<>");
        return;
    }

    // Convert character to string and delegate to outToken for escaping
    char in[2];
    in[0] = c;
    in[1] = '\0';
    outToken(str, in);
}
```