# get_encoding_name_for_icu

## Location
[src/common/encnames.c:472-484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L472-L484)

## Overview
Returns the ICU-specific encoding name string for a given PostgreSQL character encoding identifier.

## Definition
const char *get_encoding_name_for_icu(int encoding)

## Detailed Description
This function provides a mapping from PostgreSQL's internal character encoding identifiers to the corresponding encoding names used by the ICU library. It first validates that the encoding is a valid backend encoding, then returns the ICU encoding name from the pg_enc2icu_tbl lookup table. If the encoding is invalid or not supported by ICU, it returns NULL.

## Parameters / Member Variables
- encoding: Integer identifier representing a PostgreSQL character encoding (from the pg_enc enum)

## Dependencies
- Functions called/Symbols referenced:
  - PG_VALID_BE_ENCODING (macro for validating backend encodings)
  - pg_enc2icu_tbl (global mapping table from PostgreSQL encodings to ICU encoding names)
- Called from (representative examples):
  - [init_icu_converter](../i/init_icu_converter.md) (src/backend/utils/adt/pg_locale.c:2693)

## Notes and Other Information
- Returns NULL for invalid encodings or encodings without ICU support
- The returned string is a constant from the mapping table and should not be modified
- Used primarily for initializing ICU converters and other ICU-based operations
- Located in src/common/encnames.c:472-484