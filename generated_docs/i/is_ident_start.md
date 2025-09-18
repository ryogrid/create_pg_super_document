# is_ident_start

## Location
src/backend/utils/adt/misc.c: 828 - 845

## Overview
A static utility function that determines whether a character is valid as the starting character of a PostgreSQL identifier.

## Definition
```c
static bool is_ident_start(unsigned char c)
```

## Detailed Description
This function checks if a given character can legally begin a PostgreSQL identifier according to the SQL standard and PostgreSQL extensions. It implements the same logic as the {ident_start} character class in the PostgreSQL lexical scanner (scan.l). The function allows underscores, ASCII letters (both uppercase and lowercase), and any character with the high bit set (which may be part of a multibyte character in various encodings).

## Parameters / Member Variables
- `c`: The unsigned character to be tested for validity as an identifier starting character

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (macro for checking high-bit-set characters)
- Called from (representative examples):
  - [is_ident_cont](is_ident_cont.md)
  - [parse_ident](../p/parse_ident.md)

## Notes and Other Information
- This is a static function used internally within misc.c
- Must match the lexical rules defined in scan.l for consistency with the PostgreSQL parser
- Supports multibyte characters by accepting any high-bit-set character
- Part of PostgreSQL's identifier validation system
- Does not allow digits as starting characters (digits are allowed in continuation characters only)