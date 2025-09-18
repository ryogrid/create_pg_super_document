# FindDefaultConversionProc

## Location
src/backend/catalog/namespace.c: 4080 - 4106

## Overview
Finds the default encoding conversion procedure for converting text between two specific encodings by searching through the namespace search path.

## Definition
```c
Oid FindDefaultConversionProc(int32 for_encoding, int32 to_encoding)
```

## Detailed Description
This function searches for a default conversion procedure that can convert text from one encoding to another. It iterates through the active namespace search path, skipping the temporary namespace, and calls FindDefaultConversion for each namespace until a valid conversion procedure is found. This is typically used by the encoding conversion system when automatic conversion between client and server encodings is needed.

## Parameters / Member Variables
- `for_encoding`: The source encoding ID to convert from
- `to_encoding`: The target encoding ID to convert to

## Dependencies
- Functions called/Symbols referenced:
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [FindDefaultConversion](FindDefaultConversion.md)
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [PrepareClientEncoding](../P/PrepareClientEncoding.md)
  - [InitializeClientEncoding](../I/InitializeClientEncoding.md)
  - [pg_do_encoding_conversion](../p/pg_do_encoding_conversion.md)
  - [test_enc_conversion](../t/test_enc_conversion.md)

## Notes and Other Information
- Skips the temporary namespace when searching through the search path
- Returns InvalidOid if no suitable conversion procedure is found
- Used primarily by the multi-byte character encoding system
- Works in conjunction with FindDefaultConversion to locate conversion procedures
- Essential for automatic encoding conversion between client and server character sets