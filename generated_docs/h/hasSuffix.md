# hasSuffix

## Location
[src/bin/pg_dump/compress_io.c:164-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_io.c#L164-L178)

## Overview
This static utility function checks whether a given filename ends with a specific suffix string.

## Definition

```c
static int
hasSuffix(const char *filename, const char *suffix)
```
## Detailed Description
The `hasSuffix` function performs a suffix check on a filename string by comparing the end portion of the filename with the provided suffix. It uses string length calculations to determine the appropriate starting position for comparison and then performs a byte-wise comparison using memcmp. The function returns 1 if the filename ends with the suffix, and 0 otherwise.

The function first calculates the lengths of both the filename and suffix strings. If the filename is shorter than the suffix, it immediately returns 0 (false). Otherwise, it compares the last `suffixlen` bytes of the filename with the suffix using memcmp.

## Parameters / Member Variables
- `filename`: Pointer to the filename string to be checked
- `suffix`: Pointer to the suffix string to look for at the end of filename

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function)
  - memcmp (standard C library function)
- Called from (representative examples):
  - [InitDiscoverCompressFileHandle](../I/InitDiscoverCompressFileHandle.md) (src/bin/pg_dump/compress_io.c:254)
  - [InitDiscoverCompressFileHandle](../I/InitDiscoverCompressFileHandle.md) (src/bin/pg_dump/compress_io.c:256)
  - [InitDiscoverCompressFileHandle](../I/InitDiscoverCompressFileHandle.md) (src/bin/pg_dump/compress_io.c:258)

## Notes and Other Information
- This is a static function, meaning it's only visible within the compress_io.c file
- The function performs case-sensitive comparison
- Used primarily for detecting compressed file extensions (e.g., .gz, .lz4, .zst)
- Returns an integer (0 or 1) rather than a boolean type for compatibility with C89/90
- The function handles edge cases where the filename is shorter than the suffix
- Located in src/bin/pg_dump/compress_io.c at lines 164-178