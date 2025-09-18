# tarChecksum

## Location
src/port/tar.c: 90 - 113

## Overview
A function that calculates the POSIX-compliant checksum for a tar header block, used for validating and creating tar archive headers.

## Definition
```c
int tarChecksum(char *header)
```

## Detailed Description
This function computes the checksum for a tar header according to the POSIX standard. The checksum is calculated as the simple sum of all bytes in the 512-byte header, treating all bytes as unsigned values. Crucially, the checksum field itself (located at bytes 148-155, an 8-byte field) is treated as if it contained 8 space characters during the calculation, regardless of its actual contents.

The algorithm adds 8 * ' ' (8 times the ASCII value of space, which is 32) to account for the checksum field, then sums all other bytes in the header. This creates a value that can be compared against the stored checksum to verify header integrity.

## Parameters / Member Variables
- `header`: Pointer to the 512-byte tar header buffer for which to calculate the checksum

## Dependencies
- Functions called/Symbols referenced: None (uses only basic arithmetic operations)
- Called from (representative examples):
  - tar_close (in walmethods.c for finalizing tar archives)
  - [isValidTarHeader](../i/isValidTarHeader.md) (for header validation in pg_backup_tar.c)
  - [_tarGetHeader](_tarGetHeader.md) (for header processing during tar extraction)
  - tarCreateHeader (for setting the checksum when creating new headers)

## Notes and Other Information
- Always assumes a 512-byte header as per the tar standard
- The checksum field occupies bytes 148-155 (8 bytes) and is excluded from the actual byte summation
- Uses bitwise AND with 0xFF to ensure bytes are treated as unsigned values (0-255 range)
- Returns an integer that should match the checksum stored in the header for valid tar files
- Essential for both creating valid tar headers and validating existing ones in PostgreSQL's backup utilities
- Part of PostgreSQL's portable tar implementation used across various backup and restore tools