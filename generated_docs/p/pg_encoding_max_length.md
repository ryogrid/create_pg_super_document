# pg_encoding_max_length

## Location
src/common/wchar.c: 2213 - 2224

## Overview
Returns the maximum byte length that any single character can occupy in the specified encoding.

## Definition
```c
int pg_encoding_max_length(int encoding)
```

## Detailed Description
This function provides the maximum number of bytes that any single character can occupy in the specified character encoding. This information is crucial for buffer allocation, memory management, and various string processing operations where you need to account for the worst-case scenario of character storage requirements.

The function accesses the `maxmblen` field from the encoding's entry in the `pg_wchar_table` array. Different encodings have different maximum character lengths:
- Single-byte encodings (ASCII, Latin1, etc.): 1 byte
- Most Asian multibyte encodings (SJIS, BIG5, GBK, UHC): 2 bytes  
- Some complex encodings (JOHAB): 3 bytes
- UTF-8 and GB18030: 4 bytes

The function includes both an Assert and a runtime check for encoding validity to handle compiler warning issues with some MinGW versions while maintaining safety.

## Parameters / Member Variables
- `encoding`: The character encoding identifier for which to get the maximum character length

## Dependencies
- Functions called/Symbols referenced:
  - `PG_VALID_ENCODING`: Macro to validate encoding identifier (used in Assert and runtime check)
  - `PG_SQL_ASCII`: Fallback encoding constant used when invalid encoding is provided
  - `pg_wchar_table[].maxmblen`: Maximum character length field for the encoding

- Called from (representative examples):
  - [CopyConvertBuf](../C/CopyConvertBuf.md): Buffer sizing for COPY operations (copyfromparse.c)
  - [type_maximum_size](../t/type_maximum_size.md): Maximum size calculation for data types (format_type.c)
  - [ascii](../a/ascii.md), `chr`: String manipulation functions in Oracle compatibility layer
  - [pg_encoding_mbcliplen](pg_encoding_mbcliplen.md): String clipping with multibyte awareness (mbutils.c)
  - [pg_verify_mbstr_len](pg_verify_mbstr_len.md): String length verification functions
  - [parse_identifier](parse_identifier.md): Identifier parsing in psql tab completion
  - `MIN_RIGHT_CUT`: Result formatting in libpq protocol handling

## Notes and Other Information
- This function is defined in src/common/wchar.c:2213-2224
- Essential for proper buffer allocation when working with multibyte strings
- Used extensively in memory management and string processing throughout PostgreSQL
- The dual validation (Assert + runtime check) handles compiler-specific warning issues while maintaining safety
- Maximum lengths by encoding family:
  - ASCII/Latin family: 1 byte
  - Most Asian encodings: 2 bytes
  - UTF-8 and GB18030: 4 bytes maximum
  - JOHAB: 3 bytes maximum
- Critical for preventing buffer overflows in character processing operations
- Used in both server-side and client-side code for consistent character handling