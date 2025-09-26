# RS_execute

## Location
[src/backend/tsearch/regis.c:213-257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L213-L257)

## Overview
Executes a compiled regular expression pattern (Regis) against a given string to determine if the string matches the pattern.

## Definition
bool RS_execute(Regis *r, char *str)

## Detailed Description
RS_execute performs pattern matching using a simplified regular expression engine designed for PostgreSQL's text search functionality. The function traverses a linked list of RegisNode structures that represent the compiled pattern, checking each character position in the input string against the pattern rules.

The function supports two main pattern types:
- RSF_ONEOF: Character must be one of the specified characters
- RSF_NONEOF: Character must not be any of the specified characters

The function handles multibyte characters correctly using pg_mblen() and supports both prefix and suffix matching based on the issuffix flag in the Regis structure. For suffix matching, it positions the comparison at the end of the string minus the pattern length.

## Parameters / Member Variables
- : Pointer to a compiled Regis structure containing the pattern to match
- : Input string to test against the pattern (null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mblen](../p/pg_mblen.md) (for multibyte character length calculation)
  - [mb_strchr](../m/mb_strchr.md) (for multibyte-aware character search)
  - elog (for error reporting)
- Data structures used:
  - [Regis](Regis.md) (pattern container structure)
  - [RegisNode](RegisNode.md) (linked list nodes representing pattern elements)
  - RSF_ONEOF, RSF_NONEOF (pattern type constants)
- Called from (representative examples):
  - [CheckAffix](../C/CheckAffix.md) (in src/backend/tsearch/spell.c)

## Notes and Other Information
- Returns true if the string matches the pattern, false otherwise
- Performs early length check - returns false immediately if string is shorter than required pattern length
- Supports multibyte character encodings through pg_mblen() usage
- Part of PostgreSQL's text search (tsearch) subsystem, specifically used for ISpell dictionary functionality
- The function assumes the Regis structure has been properly compiled using RS_compile()
- Error handling includes logging unrecognized node types with elog(ERROR)