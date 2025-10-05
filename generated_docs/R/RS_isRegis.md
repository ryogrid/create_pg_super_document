# RS_isRegis

## Location
[src/backend/tsearch/regis.c:31-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/regis.c#L31-L73)

## Overview
Tests whether a regular expression string conforms to the limited subset of regex syntax supported by PostgreSQL's text search system.

## Definition

```c
bool
RS_isRegis(const char *str)
```
## Detailed Description
RS_isRegis validates that a regular expression string uses only the simplified regex syntax supported by PostgreSQL's text search functionality. It implements a finite state machine to parse the input string and ensure it contains only:
- Alphabetic characters (via t_isalpha)
- Character classes in brackets [abc]
- Negated character classes [^abc]

The function processes the string character by character, transitioning between states (RS_IN_WAIT, RS_IN_ONEOF, RS_IN_ONEOF_IN, RS_IN_NONEOF) to validate the structure. It must be kept in sync with RS_compile which actually compiles the validated patterns.

## Parameters / Member Variables
- `*str`: The regular expression string to validate
## Dependencies
- Functions called/Symbols referenced:
  - [t_isalpha](../t/t_isalpha.md) (check if character is alphabetic)
  - t_iseq (check character equality)
  - [pg_mblen](../p/pg_mblen.md) (get multibyte character length)
  - elog (error logging)
- State constants:
  - RS_IN_WAIT (waiting for next pattern element)
  - RS_IN_ONEOF (inside character class [])
  - RS_IN_ONEOF_IN (inside character class with characters)
  - RS_IN_NONEOF (inside negated character class [^])
- Called from:
  - [NIAddAffix](../N/NIAddAffix.md) (in spell.c:706)

## Notes and Other Information
- Returns true if the regex conforms to the supported subset, false otherwise
- Uses multibyte-aware character processing via pg_mblen
- Must be kept synchronized with RS_compile implementation
- Part of PostgreSQL's text search regex compilation system
- Designed for simple pattern matching, not full regex functionality

## Simplified Source

```c
bool RS_isRegis(const char *str) {
    int state = RS_IN_WAIT;
    const char *c = str;

    // Parse string using finite state machine
    while (*c) {
        if (state == RS_IN_WAIT) {
            if (t_isalpha(c))
                /* alphabetic character - OK */;
            else if (t_iseq(c, '['))
                state = RS_IN_ONEOF;  // Start character class
            else
                return false;
        }
        else if (state == RS_IN_ONEOF) {
            if (t_iseq(c, '^'))
                state = RS_IN_NONEOF;  // Negated character class
            else if (t_isalpha(c))
                state = RS_IN_ONEOF_IN;
            else
                return false;
        }
        else if (state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF) {
            if (t_isalpha(c))
                /* more characters in class - OK */;
            else if (t_iseq(c, ']'))
                state = RS_IN_WAIT;  // End character class
            else
                return false;
        }
        else
            elog(ERROR, "internal error in RS_isRegis: state %d", state);

        c += pg_mblen(c);  // Advance to next multibyte character
    }

    return (state == RS_IN_WAIT);  // Must end in waiting state
}
```