# pg_ultostr_zeropad

## Location
src/backend/utils/adt/numutils.c: 1269 - 1308

## Overview
Converts an unsigned 32-bit integer to its decimal string representation with zero-padding to ensure minimum width, optimized for building multi-component strings without NUL termination.

## Definition


## Detailed Description
This function converts a uint32 value into a decimal string representation and stores it at the provided memory location, ensuring the result has at least the specified minimum width by prefixing with zeros if necessary. The function is specifically designed for building composite strings containing multiple numbers, such as time formatting (HH:MM:SS). It includes an optimization for the common case of 2-digit formatting with values under 100, using a pre-computed DIGIT_TABLE for faster conversion. Unlike standard string functions, it does not write a NUL terminator, allowing for efficient concatenation of multiple numeric components.

## Parameters / Member Variables
- : Pointer to the destination buffer where the string representation will be written (caller must ensure sufficient space)
- : The unsigned 32-bit integer value to convert to string
- : Minimum width of the resulting string; shorter results are zero-padded on the left

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ultoa_n](pg_ultoa_n.md) (core conversion function)
  - DIGIT_TABLE (lookup table for digit pairs)
  - memcpy (for optimized digit copying)
  - memmove (for shifting digits when padding needed)
  - memset (for zero-padding)
- Called from (representative examples):
  - [AppendSeconds](../A/AppendSeconds.md) (datetime.c:453)
  - EncodeTimezone (datetime.c:4206, 4208, 4210, 4214, 4216, 4219)
  - [EncodeDateOnly](../E/EncodeDateOnly.md) (datetime.c:4236, 4239, 4241, 4248, 4250, 4254, 4256, 4259, 4265, 4267, 4269, 4278, 4280, 4284, 4286, 4289)
  - EncodeTimeOnly (datetime.c:4314, 4316)
  - [EncodeDateTime](../E/EncodeDateTime.md) (datetime.c:4359, 4362, 4364, 4366, 4368, 4379, 4381, 4385, 4387, 4390, 4393, 4395, 4419, 4421, 4423, 4426, 4428, 4454, 4464, 4467, 4469, 4473)

## Notes and Other Information
- Returns a pointer to the position immediately after the last written character (not NUL-terminated)
- Optimized fast path for common case of 2-digit zero-padding with values < 100
- Primarily used in datetime formatting functions throughout PostgreSQL
- Caller responsibility to ensure destination buffer has adequate space
- Designed for efficient string building patterns where multiple numeric components are concatenated
- Assert ensures minwidth > 0 for input validation