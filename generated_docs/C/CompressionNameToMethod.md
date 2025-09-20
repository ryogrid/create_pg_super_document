# CompressionNameToMethod

## Location
[src/backend/access/common/toast_compression.c:285-303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/toast_compression.c#L285-L303)

## Overview
Converts a compression method name string to its corresponding compression method identifier, searching through available built-in compression methods.

## Definition

```c
char
CompressionNameToMethod(const char *compression)
```
## Detailed Description
This function maps string names of compression methods to their internal numeric identifiers. It currently supports two built-in compression methods: "pglz" (PostgreSQL's native LZ compression) and "lz4" (LZ4 compression). The function performs string comparisons to identify the requested compression method. For LZ4, it includes conditional compilation checks to ensure LZ4 support is available. If the provided compression name is not recognized, it returns InvalidCompressionMethod.

## Parameters / Member Variables
- : String name of the compression method to look up (e.g., "pglz", "lz4")

## Dependencies
- Functions called/Symbols referenced:
  - TOAST_PGLZ_COMPRESSION
  - TOAST_LZ4_COMPRESSION
  - NO_LZ4_SUPPORT
  - InvalidCompressionMethod
- Called from (representative examples):
  - [GetAttributeCompression](../G/GetAttributeCompression.md)
  - CompressionMethodIsValid

## Notes and Other Information
- Located in src/backend/access/common/toast_compression.c:285-303
- Returns char type representing the compression method ID
- Includes conditional compilation for LZ4 support via USE_LZ4 preprocessor directive
- Part of PostgreSQL's TOAST compression infrastructure
- Case-sensitive string matching for compression method names