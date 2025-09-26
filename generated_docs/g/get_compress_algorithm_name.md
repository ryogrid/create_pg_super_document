# get_compress_algorithm_name

## Location
src/common/compression.c: 69 - 106

## Overview
A utility function that converts a  enumeration value to its corresponding human-readable string representation.

## Definition


## Detailed Description
The  function performs the inverse operation of  by converting a  enumeration value back to its string representation. It uses a switch statement to map each compression algorithm constant to its corresponding name string. The function includes an assertion to catch unexpected enumeration values during development and returns a placeholder string ("???") to satisfy compiler requirements.

## Parameters / Member Variables
- : A  enumeration value specifying which compression algorithm name to retrieve

## Dependencies
- Functions called/Symbols referenced:
  -  (enumeration type)
  -  (enumeration constant)
  -  (enumeration constant)
  -  (enumeration constant)
  -  (enumeration constant)
  -  (debugging macro)
- Called from (representative examples):
  -  (src/bin/pg_dump/compress_io.c:110)
  -  (src/bin/pg_dump/pg_backup_archiver.c:1307)
  -  (src/common/compression.c:380, 387, 398, 409)

## Notes and Other Information
- Returns a constant string pointer to the algorithm name
- Supported algorithm names: "none", "gzip", "lz4", "zstd"
- Includes no default case in the switch statement to provoke compiler warnings when new enumeration values are added
- Uses Assert(false) to catch unexpected enumeration values during debugging
- Returns "???" as a fallback to satisfy compiler requirements, though this should never be reached in normal operation
- Primarily used for error reporting, logging, and user interface display purposes
- Located in src/common/compression.c for use by both backend and frontend code