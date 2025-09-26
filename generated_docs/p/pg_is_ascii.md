# pg_is_ascii

## Location
[src/common/string.c:133-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/string.c#L133-L154)

## Overview
A utility function that determines whether a string contains only ASCII characters by checking if any bytes have the high bit set.

## Definition
```c
bool pg_is_ascii(const char *str)
```

## Detailed Description
This function iterates through each character in the input string and uses the  macro to check if any character has its most significant bit set. Since ASCII characters are defined as 7-bit values (0-127), any character with the high bit set (128-255) indicates the presence of extended ASCII or multi-byte character encodings.

The function provides a fast, efficient way to validate that a string contains only standard ASCII characters, which is important for various PostgreSQL operations including locale validation, collation processing, and SASL preparation.

## Parameters / Member Variables
- : The null-terminated input string to check for ASCII-only content

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET (PostgreSQL macro for checking high bit)
- Called from (representative examples):
  - pg_import_system_collations (src/backend/commands/collationcmds.c:1008)
  - check_locale (src/backend/utils/adt/pg_locale.c:322, 354)
  - check_locale_name (src/bin/initdb/initdb.c:2190, 2236)
  - pg_saslprep (src/common/saslprep.c:1069)

## Notes and Other Information
- Returns  if the entire string consists only of ASCII characters (0-127)
- Returns  immediately upon encountering the first non-ASCII character
- Commonly used in locale and collation validation where ASCII-only strings have special handling
- The function is optimized for early exit, stopping as soon as a non-ASCII character is found
- Essential for SASL preparation and authentication processes where character encoding matters
- Used extensively during database initialization to validate locale names and settings