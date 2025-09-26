# parse_compress_algorithm

## Location
src/common/compression.c: 49 - 68

## Overview
A utility function that parses a compression algorithm name string and converts it to the corresponding  enumeration value.

## Definition


## Detailed Description
The  function looks up a compression algorithm by its string name and sets the corresponding enumeration value. It supports the standard compression algorithms available in PostgreSQL: "none", "gzip", "lz4", and "zstd". The function performs case-sensitive string comparison to identify the algorithm and returns a boolean indicating whether the name was successfully recognized.

## Parameters / Member Variables
- : A null-terminated string containing the compression algorithm name to parse
- : A pointer to a  variable where the parsed algorithm value will be stored

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (enumeration type)
  -  (enumeration constant)
  -  (enumeration constant)
  -  (enumeration constant)
  -  (enumeration constant)
- Called from (representative examples):
  -  (src/backend/backup/basebackup.c:902)
  -  (src/bin/pg_basebackup/pg_basebackup.c:2654)
  -  (src/bin/pg_receivewal/pg_receivewal.c:800)
  -  (src/bin/pg_dump/pg_dump.c:794)

## Notes and Other Information
- Returns  if the algorithm name is recognized and successfully parsed,  otherwise
- The function performs exact case-sensitive string matching
- Supports four compression algorithms: none, gzip, lz4, and zstd
- Used primarily in command-line tools and backup-related functionality to parse user-specified compression options
- Located in src/common/compression.c, making it available to both backend and frontend code