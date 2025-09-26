# RS_compile

## Location
[src/backend/tsearch/regis.c:85-165](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L85-L165)

## Overview
Compiles a regular expression string into a linked list of RegisNode structures for efficient pattern matching in PostgreSQL's text search system.

## Definition

```c
void
RS_compile(Regis *r, bool issuffix, const char *str)
```
## Detailed Description
RS_compile parses and compiles a simplified regular expression pattern into an internal representation using a finite state machine. The function processes the input string character by character, creating a linked list of RegisNode structures that represent the pattern components. It supports:
- Individual alphabetic characters
- Character classes [abc] (RSF_ONEOF type)
- Negated character classes [^abc] (RSF_NONEOF type)

The compiled pattern is stored in the Regis structure, which tracks whether it's a suffix pattern and maintains the count of characters in the pattern. The function must be kept synchronized with RS_isRegis for validation.

## Parameters / Member Variables
- : Pointer to Regis structure to store the compiled pattern
- : Boolean indicating whether this is a suffix pattern
- : The regular expression string to compile

## Dependencies
- Functions called/Symbols referenced:
  - [newRegisNode](../n/newRegisNode.md) (create new pattern nodes)
  - [t_isalpha](../t/t_isalpha.md) (check if character is alphabetic)
  - t_iseq (check character equality)
  - [pg_mblen](../p/pg_mblen.md) (get multibyte character length)
  - COPYCHAR (copy multibyte character)
  - memset (memory initialization)
  - strlen (string length)
  - elog (error logging)
- Types and constants:
  - [Regis](Regis.md) (main regex structure)
  - [RegisNode](RegisNode.md) (pattern node structure)
  - RSF_ONEOF, RSF_NONEOF (pattern type flags)
  - State constants: RS_IN_WAIT, RS_IN_ONEOF, RS_IN_ONEOF_IN, RS_IN_NONEOF
- Called from:
  - [NIAddAffix](../N/NIAddAffix.md) (in spell.c:710)

## Notes and Other Information
- Initializes the Regis structure with memset before compilation
- Maintains a character count (nchar) for the compiled pattern
- Uses multibyte-aware character processing for international text support
- Validates input patterns and throws errors for invalid syntax
- Must end in RS_IN_WAIT state for valid patterns
- Part of PostgreSQL's text search regex compilation infrastructure
- Works in tandem with RS_isRegis for pattern validation