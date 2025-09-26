# parse_compress_specification

## Location
src/common/compression.c: 107 - 274

## Overview
A comprehensive parser that processes compression specification strings into structured  objects, supporting algorithm-specific options and parameters.

## Definition


## Detailed Description
The  function parses a compression specification string for a specified algorithm and populates a  result structure. The function handles both simple bare integer compression levels and complex comma-separated keyword=value pairs. It sets appropriate default compression levels based on the algorithm type and validates build-time support for compression libraries. The parser supports compression options like "level", "workers", and "long" (long-distance mode), and provides detailed error reporting through the parse_error field.

## Parameters / Member Variables
- : The  enumeration specifying which compression algorithm to configure
- : A null-terminated string containing the compression specification to parse (can be NULL for defaults)
- : A pointer to a  structure that will be populated with parsed values

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL string formatting function)
  -  (PostgreSQL string duplication function)
  -  (PostgreSQL memory allocation function)
  -  (PostgreSQL memory deallocation function)
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (utility function for parsing integer values)
  -  (utility function for parsing boolean values)
  -  (enumeration type)
  -  (structure type)
  - , , ,  (enumeration constants)
  - ,  (option flag constants)
- Called from (representative examples):
  -  (src/backend/backup/basebackup.c:967)
  -  (src/bin/pg_basebackup/pg_basebackup.c:2658)
  -  (src/bin/pg_receivewal/pg_receivewal.c:805)
  -  (src/bin/pg_dump/pg_dump.c:799)

## Notes and Other Information
- Initializes all fields of the result structure, including setting parse_error to NULL on success
- Sets algorithm-specific default compression levels (0 for LZ4/none, ZSTD_CLEVEL_DEFAULT for ZSTD, Z_DEFAULT_COMPRESSION for gzip)
- Checks compile-time support for compression libraries and reports errors if unavailable
- Supports bare integer specifications (e.g., "6") as shorthand for compression level
- Parses comma-separated keyword=value pairs for advanced options
- Supported keywords: "level" (compression level), "workers" (parallel workers), "long" (long-distance mode)
- Provides detailed error messages for invalid specifications
- Memory management includes proper cleanup of allocated keyword/value strings
- Located in src/common/compression.c for use by both backend and frontend code
- Should be followed by  to ensure semantic correctness of parsed values