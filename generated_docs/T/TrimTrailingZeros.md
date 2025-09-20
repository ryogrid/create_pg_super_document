# TrimTrailingZeros

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:722-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L722-L752)

## Overview
Removes trailing zeros from a numeric string while preserving at least 2 fractional digits to maintain proper decimal formatting.

## Definition

```c
void
TrimTrailingZeros(char *str)
```
## Detailed Description
TrimTrailingZeros is a utility function that removes unnecessary trailing zeros from the end of a numeric string representation. The function is designed to clean up decimal numbers by removing trailing zeros while ensuring that at least 2 fractional digits remain after the decimal point. This preserves the visual consistency of decimal formatting while removing unnecessary precision.

The function works by scanning backwards from the end of the string, removing '0' characters until it encounters either a non-zero digit or reaches a position where only 2 fractional digits would remain (indicated by checking if the character 3 positions back is a decimal point).

## Parameters / Member Variables
- : Null-terminated string containing a numeric value that may have trailing zeros to be removed

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library function for string length)

- Called from (representative examples):
  - [EncodeDateTime](../E/EncodeDateTime.md) (multiple calls in src/interfaces/ecpg/pgtypeslib/dt_common.c at lines 781, 819, 866, 914)
  - [AppendSeconds](../A/AppendSeconds.md) (src/interfaces/ecpg/pgtypeslib/interval.c:748)

## Notes and Other Information
- The function modifies the input string in-place by truncating it
- Maintains at least 2 fractional digits by checking if the character 3 positions back from the current end is a decimal point
- Primarily used in ECPG (Embedded C for PostgreSQL) for formatting datetime and interval values
- The function assumes the input string represents a valid decimal number with a decimal point
- No bounds checking is performed - the caller must ensure the string is properly formatted