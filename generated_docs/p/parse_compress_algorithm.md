# parse_compress_algorithm

## Location
[src/common/compression.c:49-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/compression.c#L49-L68)

## Overview
A utility function that parses a compression algorithm name string and converts it to the corresponding  enumeration value.

## Definition

```c
bool
parse_compress_algorithm(char *name, pg_compress_algorithm *algorithm)
```
## Detailed Description
The  function looks up a compression algorithm by its string name and sets the corresponding enumeration value. It supports the standard compression algorithms available in PostgreSQL: "none", "gzip", "lz4", and "zstd". The function performs case-sensitive string comparison to identify the algorithm and returns a boolean indicating whether the name was successfully recognized.

## Parameters / Member Variables
- `*name`: A null-terminated string containing the compression algorithm name to parse
- `*algorithm`: A pointer to a  variable where the parsed algorithm value will be stored
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

## Simplified Source

```c
// Simplified version of parse_compress_algorithm
bool parse_compress_algorithm(char *name, pg_compress_algorithm *algorithm) {
    // Simple string-to-enum mapping
    if (strcmp(name, "none") == 0)
        *algorithm = PG_COMPRESSION_NONE;
    else if (strcmp(name, "gzip") == 0)
        *algorithm = PG_COMPRESSION_GZIP;
    else if (strcmp(name, "lz4") == 0)
        *algorithm = PG_COMPRESSION_LZ4;
    else if (strcmp(name, "zstd") == 0)
        *algorithm = PG_COMPRESSION_ZSTD;
    else
        return false; // Unknown algorithm

    return true; // Successfully parsed
}
```

Key simplifications made:
- Streamlined if-else chain for clarity
- Removed detailed comments while preserving logic
- Maintained case-sensitive string comparison
- Preserved all supported compression algorithms
- Kept simple boolean return pattern