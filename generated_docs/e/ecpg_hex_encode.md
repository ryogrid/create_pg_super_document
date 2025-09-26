# ecpg_hex_encode

## Location
src/interfaces/ecpg/ecpglib/data.c: 191 - 205

## Overview
Converts binary data to hexadecimal string representation for ECPG (Embedded SQL in C) applications.

## Definition

```c
unsigned
ecpg_hex_encode(const char *src, unsigned len, char *dst)
```
## Detailed Description
This function performs binary-to-hexadecimal encoding by converting each byte of the input binary data into two hexadecimal characters. The function uses a static lookup table containing the characters '0123456789abcdef' for efficient conversion. Each input byte is split into its high and low 4-bit nibbles, which are then mapped to their corresponding hexadecimal characters and written to the destination buffer.

The encoding process iterates through the source data, extracting the upper nibble using a right shift and bit mask operation , and the lower nibble using just a bit mask . Both nibbles are used as indices into the hexadecimal character table to produce the two-character hex representation of each byte.

## Parameters / Member Variables
- : Pointer to the source binary data to be encoded
- : Length of the source data in bytes
- : Pointer to the destination buffer where hexadecimal string will be written (must be at least len*2 bytes)

## Dependencies
- Functions called/Symbols referenced: (none - uses only basic C operations and static lookup table)
- Called from (representative examples):
  - convert_bytea_to_string (in src/interfaces/ecpg/ecpglib/execute.c:499)
  - print_param_value (in src/interfaces/ecpg/ecpglib/execute.c:1090)

## Notes and Other Information
- Returns the length of the hexadecimal string produced (always len * 2)
- The destination buffer must be pre-allocated with sufficient space (at least len * 2 bytes)
- Uses lowercase hexadecimal characters (a-f) rather than uppercase
- This is a utility function specifically designed for ECPG's needs in handling binary data conversion for SQL operations
- The function does not null-terminate the output string - the caller is responsible for this if needed