# valid_restrict_key

## Location
[src/bin/pg_dump/dumputils.c:955-960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L955-L960)

## Overview
valid_restrict_key validates that a given restrict key contains only alphanumeric characters and is suitable for use with psql's \restrict and \unrestrict meta-commands.

## Definition


## Detailed Description
This function performs validation on a restrict key string to ensure it meets the requirements for use with PostgreSQL's psql restrict/unrestrict functionality. The validation checks three conditions: the key must not be NULL, must not be an empty string, and must contain only characters from the allowed alphanumeric character set.

The function uses strspn() to count how many characters from the beginning of the string are found in the restrict_chars character set, then compares this count with the total string length. If they match, all characters in the string are valid.

## Parameters / Member Variables
- : The restrict key string to validate (const char pointer)

## Dependencies
- Functions called/Symbols referenced:
  - strspn: Counts characters in restrict_key that are present in restrict_chars
  - strlen: Gets the total length of the restrict_key string
  - restrict_chars: Static constant string containing valid characters "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

- Called from (representative examples):
  - [main](../m/main.md) (pg_dump): Used to validate restrict keys in the pg_dump utility
  - [main](../m/main.md) (pg_dumpall): Used to validate restrict keys in the pg_dumpall utility
  - [main](../m/main.md) (pg_restore): Used to validate restrict keys in the pg_restore utility

## Notes and Other Information
- Returns true if the restrict key is valid, false otherwise
- The validation ensures compatibility with psql's restrict/unrestrict commands
- Empty strings and NULL pointers are considered invalid
- Only alphanumeric characters (a-z, A-Z, 0-9) are allowed in valid restrict keys
- This function is typically used in conjunction with generate_restrict_key() for security validation
- Located at src/bin/pg_dump/dumputils.c:955-960
- Part of the PostgreSQL dump utility suite's security framework