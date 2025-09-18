# pg_detoast_datum_packed

## Location
src/backend/utils/fmgr/fmgr.c: 1864 - 1887

## Overview
This function conditionally detoasts a varlena datum only if it is compressed or externally stored, leaving short-header (packed) format datums unchanged for optimal performance.

## Definition


## Detailed Description
pg_detoast_datum_packed provides selective detoasting that preserves the packed (short-header) format when possible. Unlike pg_detoast_datum_copy which always creates a new copy, or full detoasting which converts all extended formats to normal 4-byte header format, this function only detoasts datums that are compressed or externally stored.

This selective approach is particularly efficient when working with short values that use the 1-byte header format, as it avoids the unnecessary overhead of converting them to the 4-byte header format. The function is commonly used in scenarios where the caller can handle both short-header and normal-header formats, such as when converting text to C strings.

## Parameters / Member Variables
- : A pointer to the varlena structure that may be in various extended forms

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_COMPRESSED (macro to check if datum is compressed)
  - VARATT_IS_EXTERNAL (macro to check if datum is externally stored)
  - [detoast_attr](../d/detoast_attr.md) (function to fully detoast extended datums)
- Called from (representative examples):
  - text_to_cstring
  - text_to_cstring_buffer
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - [transform_jsonb_string_values](../t/transform_jsonb_string_values.md)
  - PG_DETOAST_DATUM_PACKED (macro)

## Notes and Other Information
This function represents an optimization for PostgreSQL's TOAST system where preserving the packed format can save both memory and processing time. Short-header varlenas (those with 1-byte headers for values up to 126 bytes) are left in their compact form, while only compressed or externally stored datums are detoasted. This is particularly beneficial for text processing functions that can work with either header format, avoiding unnecessary format conversions and memory allocations. The function is part of the family of detoasting functions that provide different trade-offs between performance and data format guarantees.