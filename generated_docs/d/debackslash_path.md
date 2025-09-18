# debackslash_path

## Location
[src/port/path.c:163-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L163-L200)

## Overview
Converts backslash ('\') characters to forward slash ('/') characters within a file path string, with special handling for multi-byte character encodings like Shift-JIS.

## Definition


## Detailed Description
This function performs in-place conversion of backslash characters to forward slashes in a path string. The conversion is encoding-aware to handle multi-byte character sets properly. For Shift-JIS encoding (PG_SJIS), it uses special logic to avoid incorrectly converting bytes that are part of multi-byte characters but happen to have the same value as a backslash (0x5C). For all other encodings, it performs a simple character-by-character replacement.

The function is designed to normalize Windows-style path separators to Unix-style separators while preserving the integrity of multi-byte character sequences.

## Parameters / Member Variables
- : The null-terminated string containing the path to be modified in-place
- : The character encoding of the path string (used to determine if special multi-byte handling is needed)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_sjis_mblen](../p/pg_sjis_mblen.md)
  - PG_SJIS
- Called from (representative examples):
  - [cleanup_path](../c/cleanup_path.md)
  - [canonicalize_path_enc](../c/canonicalize_path_enc.md)

## Notes and Other Information
- This is a static function, only accessible within src/port/path.c
- The function modifies the input path string in-place
- Special handling for Shift-JIS is necessary because this encoding can contain bytes equal to 0x5C (backslash) as part of valid multi-byte characters
- For non-SJIS encodings, the function assumes that 0x5C bytes can only represent actual backslash characters
- This function is part of PostgreSQL's path manipulation utilities for cross-platform compatibility