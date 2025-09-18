# pg_encoding_mblen

## Location
[src/common/wchar.c:2135-2146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L2135-L2146)

## Overview
Returns the byte length of a multibyte character in a specified encoding, serving as the primary interface for character length determination in PostgreSQL.

## Definition


## Detailed Description
This function determines the byte length of a multibyte character by dispatching to encoding-specific length calculation functions stored in the pg_wchar_table. It serves as the main API for multibyte character length determination across PostgreSQL's codebase, providing a unified interface for all supported character encodings.

The function includes built-in validation that falls back to SQL_ASCII behavior for invalid encoding identifiers, ensuring robust operation even with corrupted input. For most encodings, only the first byte needs to be examined to determine character length, though GB18030 is a notable exception that may examine up to two bytes.

The function is designed for use cases where the input string is either zero-terminated, known to be valid in the specified encoding, or the encoding is not GB18030. For cases that don't meet these criteria, callers should use pg_encoding_mblen_or_incomplete() instead to handle potential buffer boundary issues.

## Parameters / Member Variables
- : PostgreSQL encoding identifier (e.g., PG_UTF8, PG_EUC_JP, PG_LATIN1)
- : Pointer to the start of the multibyte character sequence

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_ENCODING (encoding validation macro)
  - pg_wchar_table (encoding function dispatch table)
  - PG_SQL_ASCII (fallback encoding constant)
- Called from (representative examples):
  - [CopyAttributeOutText](../C/CopyAttributeOutText.md) (COPY command text output)
  - [CopyAttributeOutCSV](../C/CopyAttributeOutCSV.md) (COPY command CSV output)
  - sqlchar_to_unicode (XML processing)
  - [pg_encoding_mblen_or_incomplete](pg_encoding_mblen_or_incomplete.md) (safer variant)
  - [pg_encoding_mblen_bounded](pg_encoding_mblen_bounded.md) (bounded variant)
  - [fmtIdEnc](../f/fmtIdEnc.md) (identifier encoding)
  - [PQescapeInternal](../P/PQescapeInternal.md) (libpq string escaping)
  - [PQmblen](../P/PQmblen.md) (libpq public API)

## Notes and Other Information
- Uses a dispatch table (pg_wchar_table) that maps encoding IDs to their specific mblen functions
- Falls back to SQL_ASCII behavior for invalid encoding parameters, ensuring function never fails
- For most encodings, character length can be determined from the first byte alone
- GB18030 is special case requiring examination of up to two bytes for accurate length determination
- [Result](../R/Result.md) may exceed actual remaining string length when dealing with potentially invalid text
- Callers working with untrusted input should consider using Min(remaining, result) or the safer pg_encoding_mblen_or_incomplete() variant
- Core component of PostgreSQL's multibyte character support system used throughout the codebase
- Performance-critical function optimized for fast dispatch to encoding-specific implementations