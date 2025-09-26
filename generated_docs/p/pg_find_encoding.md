# pg_find_encoding

## Location
src/backend/utils/adt/encode.c: 603 - 612

## Overview
Searches for and returns a pointer to a PostgreSQL encoding structure by name, used to look up encoding/decoding functions for different data formats.

## Definition

```c
static const struct pg_encoding *
pg_find_encoding(const char *name)
```
## Detailed Description
This function performs a case-insensitive lookup in the enclist array to find a matching encoding by name. It iterates through the statically defined list of available encodings (such as "hex", "base64", "escape") and returns a pointer to the corresponding pg_encoding structure if found. The pg_encoding structure contains function pointers for encoding length calculation, decoding length calculation, encoding, and decoding operations specific to that format.

The function supports PostgreSQL's binary data encoding formats used primarily with the bytea data type. If no matching encoding is found, it returns NULL.

## Parameters / Member Variables
- : The name of the encoding to search for (case-insensitive comparison)

## Dependencies
- Functions called/Symbols referenced:
  - pg_strcasecmp (for case-insensitive string comparison)
  - enclist (static array containing available encodings)
- Called from (representative examples):
  - binary_encode (to find encoding functions for encoding operations)
  - binary_decode (to find decoding functions for decoding operations)

## Notes and Other Information
- This is a static function, only accessible within the encode.c file
- Returns a pointer to the pg_encoding structure, not a copy
- The enclist array is terminated by an entry with a NULL name field
- Supports case-insensitive lookup (e.g., "HEX", "hex", "Hex" all match)
- Currently supports "hex", "base64", and "escape" encoding formats
- The returned structure contains function pointers for all four operations: encode_len, decode_len, encode, and decode
- Used as part of PostgreSQL's binary_encode() and binary_decode() SQL functions