# EncodeSpecialInterval

## Location
[src/backend/utils/adt/timestamp.c:1607-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1607-L1617)

## Overview
Converts special reserved interval values (positive and negative infinity) to their corresponding string representations for output formatting.

## Definition

```c
static void
EncodeSpecialInterval(const Interval *interval, char *str)
```
## Detailed Description
This static utility function handles the string encoding of PostgreSQL's special interval values that represent infinite durations. Similar to its timestamp counterpart, it processes two reserved interval values:

1. **Negative infinity interval** ("beginning of time") - encoded as the string constant EARLY
2. **Positive infinity interval** ("end of time") - encoded as the string constant LATE

The function performs a simple conditional check using PostgreSQL's interval classification macros and copies the appropriate constant string to the provided buffer. If called with a regular (finite) interval value, it triggers an error since this function is only intended for special infinite interval values.

This is the interval-specific version of the timestamp encoding function, designed to maintain consistency in how infinite values are represented across PostgreSQL's temporal data types.

## Parameters / Member Variables
- : Pointer to the Interval value to be encoded (must be a special infinite value)
- : Output buffer where the string representation will be stored (caller must ensure adequate size)

## Dependencies
- Functions called/Symbols referenced:
  - INTERVAL_IS_NOBEGIN (macro to check for negative infinity interval)
  - INTERVAL_IS_NOEND (macro to check for positive infinity interval)
  - EARLY, LATE (string constants for infinity representations)
  - strcpy (standard C string copy function)
  - elog (PostgreSQL error logging)
- Called from:
  - [interval_out](../i/interval_out.md) (interval to string conversion)

## Notes and Other Information
- Only handles special infinite interval values, not regular intervals
- Assumes output buffer has sufficient space (no bounds checking)
- Part of PostgreSQL's interval output formatting system
- [String](../S/String.md) constants EARLY and LATE are typically "-infinity" and "infinity" respectively
- Function will error if called with finite interval values
- Static function scope limits usage to within the timestamp.c compilation unit