# fill_str

## Location
src/backend/utils/adt/formatting.c: 5038 - 5044

## Overview
A simple utility function that fills a character buffer with a specified character and null-terminates it.

## Definition
```c
static char *fill_str(char *str, int c, int max)
```

## Detailed Description
`fill_str` is a straightforward utility function within PostgreSQL's formatting system that fills a character buffer with a specified character value and ensures proper null-termination. This function serves as a building block for various number formatting operations where padding or filling with specific characters is required.

The function uses `memset` to efficiently fill the buffer with the specified character, then explicitly places a null terminator at the end to create a valid C string. This approach is commonly used in formatting operations where consistent-width output strings are needed, such as zero-padding numbers or creating placeholder strings.

## Parameters / Member Variables
- `str` (char*): Pointer to the character buffer to be filled
- `c` (int): The character value to fill the buffer with (typically passed as int for memset compatibility)
- `max` (int): The number of characters to fill (not including the null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - `memset` - Standard C library function for memory filling
  - No other PostgreSQL-specific dependencies
- Called from (representative examples):
  - [int_to_roman](../i/int_to_roman.md) - Roman numeral conversion
  - [numeric_to_char](../n/numeric_to_char.md) - Numeric to string formatting
  - [int4_to_char](../i/int4_to_char.md) - Integer formatting
  - [int8_to_char](../i/int8_to_char.md) - Long integer formatting
  - [float4_to_char](float4_to_char.md) - Float formatting
  - [float8_to_char](float8_to_char.md) - Double formatting

## Notes and Other Information
- This function is part of the "NUMBER version part" section of the formatting system, as indicated by the comment above its definition
- The function is static, meaning it's only accessible within the formatting.c source file
- Returns the same pointer that was passed in, allowing for convenient chaining or assignment
- The null terminator is placed at position (str + max), making the total string length max+1 bytes
- Used extensively throughout PostgreSQL's numeric formatting functions for creating padded output strings
- Simple but essential utility that abstracts the common pattern of filling and null-terminating character buffers