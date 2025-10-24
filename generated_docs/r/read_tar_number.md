# read_tar_number

## Location
[src/port/tar.c:58-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/tar.c#L58-L89)

## Overview
A utility function that parses numeric values from tar header fields, supporting both POSIX octal format and GNU binary format for reading tar archive headers.

## Definition
```c
uint64 read_tar_number(const char *s, int len)
```

## Detailed Description
This function reads and converts a numeric value from a tar header field. It serves as the counterpart to print_tar_number, handling both the standard POSIX octal format and the GNU extension binary format. The function automatically detects the format by checking the first byte: if it starts with \200, it uses base-256 binary format (GNU extension); otherwise, it assumes POSIX octal format.

For octal format, it reads digits from '0' to '7' and stops when it encounters a non-octal digit or reaches the field length. For binary format, it reads the remaining bytes after the \200 marker as MSB-first binary data.

## Parameters / Member Variables
- `s`: Pointer to the character buffer containing the numeric field to be parsed
- `len`: Length of the field in the tar header (number of bytes to read)

## Dependencies
- Functions called/Symbols referenced: None (uses only basic bit operations and character manipulation)
- Called from (representative examples):
  - [bbstreamer_tar_header](../b/bbstreamer_tar_header.md) (multiple calls for parsing various tar header fields)
  - [isValidTarHeader](../i/isValidTarHeader.md) (for tar header validation in pg_dump)
  - [_tarGetHeader](../t/_tarGetHeader.md) (for extracting header information in pg_backup_tar.c)

## Notes and Other Information
- Returns a 64-bit unsigned integer representing the parsed value
- The function supports only non-negative numbers and doesn't handle negative values according to GNU rules
- Automatically detects between POSIX octal format (ending with space or NUL) and GNU binary format (starting with \200)
- Used primarily in PostgreSQL's backup and restore utilities for processing tar archive headers
- The function is robust against malformed input - octal parsing stops at invalid characters
- Part of the portable tar implementation that allows PostgreSQL tools to work with standard tar formats

## Simplified Source

```c
uint64 read_tar_number(const char *s, int len)
{
    uint64 result = 0;

    if (*s == '\200') {
        // GNU binary format: read MSB-first bytes
        while (--len) {
            result <<= 8;
            result |= (unsigned char)(*++s);
        }
    } else {
        // POSIX octal format: read octal digits
        while (len-- && *s >= '0' && *s <= '7') {
            result <<= 3;
            result |= (*s - '0');
            s++;
        }
    }

    return result;
}
```