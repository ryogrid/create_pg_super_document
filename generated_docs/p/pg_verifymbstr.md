# pg_verifymbstr

## Location
[src/backend/utils/mb/mbutils.c:1556-1565](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L1556-L1565)

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
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (retrieves current database encoding)
  - [pg_verify_mbstr](pg_verify_mbstr.md) (performs the actual encoding validation)
- Called from (representative examples):
  - [spg_text_leaf_consistent](../s/spg_text_leaf_consistent.md)
  - [CopyReadAttributesText](../C/CopyReadAttributesText.md)
  - [read_text_file](../r/read_text_file.md)
  - [char2wchar](../c/char2wchar.md)
  - [plperl_spi_exec](plperl_spi_exec.md)
  - [plperl_spi_query](plperl_spi_query.md)
  - [PLy_cursor_query](../P/PLy_cursor_query.md)
  - [PLy_output](../P/PLy_output.md)
  - [PLy_spi_prepare](../P/PLy_spi_prepare.md)
  - [PLyObject_AsString](../P/PLyObject_AsString.md)

## Notes and Other Information
- Returns true if the string is valid in the current database encoding, false otherwise
- The noError parameter allows flexible error handling based on context needs
- Widely used across PostgreSQL core and procedural language implementations
- Critical for maintaining data integrity when processing external text input
- Essential component in PostgreSQL's multi-byte character support infrastructure

## Simplified Source

```c
bool
pg_verifymbstr(const char *mbstr, int len, bool noError)
{
    // Verify string using current database encoding
    return pg_verify_mbstr(GetDatabaseEncoding(), mbstr, len, noError);
}
```
- Helps prevent encoding-related corruption and ensures consistent text handling