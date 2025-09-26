# ecpg_hex_dec_len

## Location
[src/interfaces/ecpg/ecpglib/data.c:134-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/data.c#L134-L139)

## Overview
Calculates the required buffer length for decoding hexadecimal-encoded data back to binary format in the ECPG library.

## Definition
```c
unsigned ecpg_hex_dec_len(unsigned srclen)
```

## Detailed Description
This function is the complement to `ecpg_hex_enc_len` and calculates how much space is needed to store the binary data that results from decoding a hexadecimal string. Since each pair of hexadecimal characters represents exactly one byte of binary data, the function divides the source length by 2 using a right bit shift operation.

Like its encoding counterpart, this function is part of PostgreSQL's ECPG (Embedded SQL in C) library and is used when converting hexadecimal string representations back to binary data (bytea) for storage or processing.

## Parameters / Member Variables
- `srclen`: The length in characters of the source hexadecimal string to be decoded

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple arithmetic operation)
- Called from (representative examples):
  - ecpg_get_data

## Notes and Other Information
- Uses bit shifting (`srclen >> 1`) instead of division for efficiency, which is equivalent to `srclen / 2`
- This function assumes the input length represents a valid hexadecimal string with an even number of characters
- No validation is performed on the input length - callers are responsible for ensuring the source data is properly formatted
- The function is imported from backend encoding utilities to maintain consistency between server and client-side encoding/decoding operations
- Part of the broader ECPG infrastructure for handling PostgreSQL bytea data types in embedded C applications