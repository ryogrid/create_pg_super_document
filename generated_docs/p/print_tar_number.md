# print_tar_number

## Location
[src/port/tar.c:22-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/tar.c#L22-L57)

## Overview
A utility function that formats numeric values into tar header fields, supporting both POSIX octal format and GNU binary format for values that don't fit in octal representation.

## Definition

```c
void
print_tar_number(char *s, int len, uint64 val)
```
## Detailed Description
This function writes a numeric value into a tar header field using the appropriate format based on the value size. It implements the POSIX standard for tar headers, which specifies that numeric fields should be written in octal format with leading zeroes and a trailing space. However, when the value is too large to fit in the available space using octal representation, it falls back to the GNU extension that uses base-256 binary format with a leading \200 byte indicator.

The function calculates whether the value fits in octal by checking if it's less than 2^((len-1)*3), which represents the maximum value that can be stored in (len-1) octal digits. If it fits, it uses octal format; otherwise, it uses the GNU binary format.

## Parameters / Member Variables
- `*s`: Pointer to the character buffer where the formatted number will be written
- `len`: Length of the field in the tar header (number of bytes available)
- `val`: The 64-bit unsigned integer value to be formatted and written
## Dependencies
- Functions called/Symbols referenced: None (uses only basic bit operations and character manipulation)
- Called from (representative examples):
  - tarCreateHeader (multiple times for various tar header fields like size, mtime, mode, uid, gid, checksum)
  - [tar_close](../t/tar_close.md) (in walmethods.c for finalizing tar archives)

## Notes and Other Information
- The function supports only non-negative numbers and doesn't handle negative values according to GNU rules
- The POSIX format uses octal representation with a trailing space, while the GNU extension uses binary MSB-first format with \200 prefix
- This is part of PostgreSQL's portable tar implementation used primarily in backup utilities like pg_basebackup
- The function modifies the buffer in-place and assumes the caller has allocated sufficient space
- The choice between octal and binary format is automatic based on the value size relative to the field width

## Simplified Source

```c
void print_tar_number(char *s, int len, uint64 val)
{
    // Check if value fits in octal format (POSIX standard)
    if (val < (((uint64)1) << ((len - 1) * 3))) {
        // Use POSIX octal format with trailing space
        s[--len] = ' ';
        while (len) {
            s[--len] = (val & 7) + '0';  // Extract octal digit
            val >>= 3;                   // Shift for next digit
        }
    } else {
        // Use GNU binary format for large values
        s[0] = '\200';  // GNU extension marker
        while (len > 1) {
            s[--len] = (val & 255);  // Store byte in MSB-first order
            val >>= 8;               // Shift for next byte
        }
    }
}
```