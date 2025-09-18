# GetCompressionMethodName

## Location
src/backend/access/common/toast_compression.c: 304 - 316

## Overview
Converts a compression method identifier to its corresponding string name, providing the reverse mapping of CompressionNameToMethod.

## Definition


## Detailed Description
This function takes a numeric compression method identifier and returns the corresponding string name. It uses a switch statement to map compression method IDs to their string representations. Currently supports two compression methods: TOAST_PGLZ_COMPRESSION (returns "pglz") and TOAST_LZ4_COMPRESSION (returns "lz4"). If an invalid or unrecognized method ID is provided, the function generates an error using elog(ERROR) and includes the invalid method character in the error message.

## Parameters / Member Variables
- : Character representing the compression method ID to convert to a name

## Dependencies
- Functions called/Symbols referenced:
  - TOAST_PGLZ_COMPRESSION
  - TOAST_LZ4_COMPRESSION
  - elog (for error reporting)
- Called from (representative examples):
  - MergeAttributes
  - transformTableLikeClause
  - CompressionMethodIsValid

## Notes and Other Information
- Located in src/backend/access/common/toast_compression.c:304-316
- Returns const char* pointing to a static string
- Generates an ERROR-level log message for invalid compression methods
- Part of PostgreSQL's TOAST compression infrastructure
- Complementary function to CompressionNameToMethod for bidirectional conversion
- The NULL return after elog(ERROR) is included to keep the compiler quiet, though it's unreachable code