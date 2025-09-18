# pg_verifymbstr

## Location
src/backend/utils/mb/mbutils.c: 1556 - 1565

## Overview
A convenience wrapper function that verifies whether a multi-byte string is validly encoded according to the current database's character encoding.

## Definition
```c
bool pg_verifymbstr(const char *mbstr, int len, bool noError)
```

## Detailed Description
This function serves as a database-encoding-aware wrapper around the more general pg_verify_mbstr() function. It automatically uses the current database's character encoding for validation, eliminating the need for callers to explicitly specify the encoding.

The function is essential for:
- Input validation when receiving text data from external sources
- Ensuring data integrity in multi-byte character processing
- Preventing invalid character sequences from corrupting the database
- Providing encoding validation with flexible error handling

By using the current database encoding, it ensures that all text data conforms to the database's expected character format, which is critical for consistent text processing, indexing, and storage operations.

## Parameters / Member Variables
- `mbstr`: Pointer to the multi-byte string to be validated
- `len`: Length of the string in bytes to validate
- `noError`: If true, returns false on invalid encoding instead of throwing an error; if false, throws an error on invalid encoding

## Dependencies
- Functions called/Symbols referenced:
  - GetDatabaseEncoding (retrieves current database encoding)
  - pg_verify_mbstr (performs the actual encoding validation)
- Called from (representative examples):
  - spg_text_leaf_consistent
  - CopyReadAttributesText
  - read_text_file
  - char2wchar
  - plperl_spi_exec
  - plperl_spi_query
  - PLy_cursor_query
  - PLy_output
  - PLy_spi_prepare
  - PLyObject_AsString

## Notes and Other Information
- Returns true if the string is valid in the current database encoding, false otherwise
- The noError parameter allows flexible error handling based on context needs
- Widely used across PostgreSQL core and procedural language implementations
- Critical for maintaining data integrity when processing external text input
- Essential component in PostgreSQL's multi-byte character support infrastructure
- Helps prevent encoding-related corruption and ensures consistent text handling