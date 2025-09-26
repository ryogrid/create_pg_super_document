# pg_ascii_toupper

## Location
[src/port/pgstrcasecmp.c:135-145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pgstrcasecmp.c#L135-L145)

## Overview
Converts a character to uppercase following strict C/POSIX locale rules, operating only on ASCII characters.

## Definition

```c
unsigned char
pg_ascii_toupper(unsigned char ch)
```
## Detailed Description
The  function provides a locale-independent way to convert ASCII characters to uppercase. Unlike , this function strictly follows C/POSIX locale rules and only operates on ASCII characters (a-z), making it suitable for situations where consistent, locale-independent behavior is required regardless of the system's locale settings.

This function performs a simple range check for lowercase ASCII characters (a-z) and converts them by adding the offset between lowercase and uppercase letters. Characters outside this range, including extended characters, are returned unchanged. This makes it particularly useful for processing identifiers, keywords, or other text that should be handled consistently across different locale environments.

The function is simpler and faster than  because it doesn't need to handle locale-specific character sets or call standard library functions.

## Parameters / Member Variables
- : The unsigned character to convert to uppercase (only ASCII a-z will be converted)

## Dependencies
- Functions called/Symbols referenced:
  - None (operates directly on character values)
- Called from (representative examples):
  - [String](../S/String.md) formatting functions (asc_toupper, asc_initcap)
  - Regular expression processing (pg_wc_toupper)
  - Event trigger command filtering (filter_list_to_array)

## Notes and Other Information
- Returns uppercase version only for ASCII lowercase letters (a-z)
- All other characters (including extended characters) are returned unchanged
- Provides locale-independent behavior following C/POSIX rules
- Faster than locale-aware alternatives since it avoids library calls
- Used when consistent ASCII-only case conversion is needed regardless of system locale
- Particularly useful for processing SQL identifiers and keywords that must follow ASCII rules