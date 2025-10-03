# generate_restrict_key

## Location
[src/bin/pg_dump/dumputils.c:931-954](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/dumputils.c#L931-L954)

## Overview
generate_restrict_key creates a random alphanumeric string for use as a restrict key with psql's \restrict and \unrestrict meta-commands.

## Definition

```c
char *
generate_restrict_key(void)
```
## Detailed Description
This function generates a 63-character random alphanumeric string that serves as a restrict key for PostgreSQL dump utilities. The function uses cryptographically strong random number generation to ensure the key is unpredictable and secure. Each character in the generated key is selected from a predefined set of alphanumeric characters (a-z, A-Z, 0-9) to ensure compatibility with psql's restrict/unrestrict commands.

The function allocates memory for a 64-byte buffer (63 characters plus null terminator) and fills it with randomly selected characters from the restrict_chars character set. The random selection process uses modulo arithmetic to map random bytes to valid character indices.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): Allocates memory for the restrict key string
  - [pg_strong_random](../p/pg_strong_random.md): Generates cryptographically strong random bytes
  - strlen: Calculates the length of the restrict_chars array
  - restrict_chars: Static constant string containing valid characters "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

- Called from (representative examples):
  - [main](../m/main.md) (pg_dump): Used in the main function of pg_dump utility
  - [main](../m/main.md) (pg_dumpall): Used in the main function of pg_dumpall utility  
  - [main](../m/main.md) (pg_restore): Used in the main function of pg_restore utility

## Notes and Other Information
- Returns NULL if pg_strong_random fails to generate random data
- The generated key is always 63 characters long plus null terminator
- Uses only alphanumeric characters for maximum compatibility
- The restrict key is used for security purposes to prevent unauthorized access during dump/restore operations
- Memory returned by this function should be freed by the caller using pg_free()
- Located at src/bin/pg_dump/dumputils.c:931-954
- Part of the PostgreSQL dump utility suite for secure backup/restore operations