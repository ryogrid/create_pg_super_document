# puttzcode

## Location
src/timezone/zic.c: 2014 - 2022

## Overview
A static utility function that writes a 32-bit integer value to a file in big-endian binary format for timezone data files.

## Definition
```c
static void puttzcode(const int32 val, FILE *const fp)
```

## Detailed Description
The `puttzcode` function takes a 32-bit integer value and writes it to the specified file stream in big-endian binary format. This function is part of PostgreSQL's timezone compilation utilities and is used for writing timezone data to binary files. It uses an internal 4-byte buffer to hold the converted value before writing it to the file stream in a single write operation.

## Parameters / Member Variables
- `val`: A 32-bit integer value (int32) to be written to the file
- `fp`: A file pointer where the binary representation will be written

## Dependencies
- Functions called/Symbols referenced:
  - convert (for converting int32 to binary representation)
  - fwrite (standard C library function for file writing)
- Called from (representative examples):
  - [puttzcodepass](puttzcodepass.md)

## Notes and Other Information
- The function uses a 4-byte buffer to hold the converted binary data
- The conversion to big-endian format is handled by the `convert` function
- This function is static and only accessible within the zic.c compilation unit
- Part of the timezone data compilation infrastructure in PostgreSQL
- The entire buffer is written to the file in a single fwrite operation