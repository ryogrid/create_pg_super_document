# ecpg_hex_enc_len

## Location
[src/interfaces/ecpg/ecpglib/data.c:128-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/data.c#L128-L133)

## Overview
Calculates the required buffer length for hexadecimal encoding of binary data in the ECPG library.

## Definition
```c
unsigned ecpg_hex_enc_len(unsigned srclen)
```

## Detailed Description
This function is imported from the PostgreSQL backend encoding utilities (src/backend/utils/adt/encode.c) and provides a simple calculation for determining how much space is needed to store the hexadecimal representation of binary data. Since each byte requires exactly 2 hexadecimal characters for representation, the function simply doubles the source length using a left bit shift operation.

The function is part of PostgreSQL's ECPG (Embedded SQL in C) library and is used when converting binary data (bytea) to its hexadecimal string representation for display or transmission purposes.

## Parameters / Member Variables
- `srclen`: The length in bytes of the source binary data to be encoded

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple arithmetic operation)
- Called from (representative examples):
  - [ecpg_get_data](ecpg_get_data.md)
  - [convert_bytea_to_string](../c/convert_bytea_to_string.md)  
  - [print_param_value](../p/print_param_value.md)

## Notes and Other Information
- Uses bit shifting (`srclen << 1`) instead of multiplication for efficiency, which is equivalent to `srclen * 2`
- This is a utility function imported from the backend to maintain consistency in encoding behavior between the server and ECPG client library
- The function assumes that no null terminator or other overhead is included in the calculation - callers must account for additional space if needed
- Part of the broader ECPG infrastructure for handling PostgreSQL data types in embedded C applications