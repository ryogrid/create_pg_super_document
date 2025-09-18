# is_ident_cont

## Location
[src/backend/utils/adt/misc.c:846-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/misc.c#L846-L860)

## Overview
A static utility function that determines whether a character is valid as a continuation character within a PostgreSQL identifier.

## Definition
```c
static bool is_ident_cont(unsigned char c)
```

## Detailed Description
This function checks if a given character can legally appear within a PostgreSQL identifier (after the first character) according to the SQL standard and PostgreSQL extensions. It implements the same logic as the {ident_cont} character class in the PostgreSQL lexical scanner (scan.l). The function allows digits, dollar signs, and any character that is valid as an identifier start character (underscores, ASCII letters, and high-bit-set characters).

## Parameters / Member Variables
- `c`: The unsigned character to be tested for validity as an identifier continuation character

## Dependencies
- Functions called/Symbols referenced:
  - [is_ident_start](is_ident_start.md) (for checking if character is valid identifier start)
- Called from (representative examples):
  - [parse_ident](../p/parse_ident.md)

## Notes and Other Information
- This is a static function used internally within misc.c
- Must match the lexical rules defined in scan.l for consistency with the PostgreSQL parser
- Extends the valid identifier start characters by also allowing digits (0-9) and dollar signs ($)
- Part of PostgreSQL's identifier validation system
- Digits and dollar signs are only allowed as continuation characters, not as starting characters
- Supports all characters that can start an identifier plus additional continuation-specific characters