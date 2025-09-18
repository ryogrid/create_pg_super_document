# parse_compress_options

## Location
[src/common/compression.c:426-476](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L426-L476)

## Overview
Parses command-line compression options (typically from -Z/--compress) into separate algorithm and detail components for further processing.

## Definition


## Detailed Description
This function performs basic parsing of compression options specified through command-line arguments. It handles two main formats:

1. **Bare integer format**: For backward compatibility, integers are interpreted as:
   - 0 = "none" algorithm with no detail
   - Any other integer = "gzip" algorithm with the integer as detail (compression level)

2. **METHOD:DETAIL format**: Splits the input on the first colon (':') character:
   - Everything before the colon becomes the algorithm name
   - Everything after the colon becomes the detail string
   - If no colon exists, the entire string is treated as the algorithm name with no detail

The parsed components are then typically passed to parse_compress_specification() for full parsing and validation. This function only performs the initial split - it does not validate the algorithm name or detail format.

## Parameters / Member Variables
- : Input compression option string from command line (e.g., "gzip:6" or "5" or "lz4")
- : Output parameter - pointer to store the parsed algorithm name (caller must free)
- : Output parameter - pointer to store the parsed detail string, or NULL if no detail (caller must free if not NULL)

## Dependencies
- Functions called/Symbols referenced:
  - strtol
  - strchr
  - [pstrdup](pstrdup.md)
  - [palloc](palloc.md)
  - memcpy
- Called from (representative examples):
  - [backup_parse_compress_options](../b/backup_parse_compress_options.md) (src/bin/pg_basebackup/pg_basebackup.c:1006)
  - [main](../m/main.md) (src/bin/pg_basebackup/pg_receivewal.c:728)
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:603)

## Notes and Other Information
- The function allocates memory for the algorithm and detail strings using pstrdup() and palloc()
- Caller is responsible for freeing the allocated memory for both algorithm and detail strings
- Maintains backward compatibility with integer-only compression specifications
- Does not perform validation of algorithm names or detail formats - this is handled by subsequent parsing functions
- Used as a preprocessing step before calling parse_compress_specification() for full validation