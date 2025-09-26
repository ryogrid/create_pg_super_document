# detzcode64

## Location
src/timezone/localtime.c: 144 - 169

## Overview
Decodes an 8-byte big-endian signed 64-bit integer from a byte array, handling two's-complement representation across different machine architectures.

## Definition

```c
union input_buffer
{
	/* The first part of the buffer, interpreted as a header.  */
	struct tzhead tzhead;

	/* The entire buffer.  */
	char		buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
					+ 4 * TZ_MAX_TIMES];
};
```
## Detailed Description
The `detzcode64` function is the 64-bit version of `detzcode`, designed to handle larger integer values in timezone data files. It reads 8 bytes from the input pointer and reconstructs a 64-bit signed integer value, maintaining compatibility with big-endian timezone file formats while ensuring correct two's-complement representation across different machine architectures.

Like its 32-bit counterpart, this function includes careful handling of the sign bit and special logic to prevent overflow when dealing with the most negative representable 64-bit integer value.

## Parameters / Member Variables
- `codep`: Pointer to an 8-byte array containing the big-endian encoded signed 64-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - TWOS_COMPLEMENT (macro for architecture detection, used twice at lines 151 and 163)
- Called from (representative examples):
  - tzloadbody (calls at lines 295, 353)

## Notes and Other Information
- This is a static function, only accessible within the localtime.c compilation unit
- Processes 8 bytes instead of the 4 bytes handled by `detzcode`
- Uses uint64 for intermediate calculations to avoid signed integer overflow during bit manipulation
- Essential for parsing newer timezone file formats that require 64-bit timestamp precision
- Includes the same overflow protection as `detzcode` but adapted for 64-bit arithmetic
- The function is used less frequently than `detzcode` but is critical for handling timezone data with dates beyond the 32-bit time_t range