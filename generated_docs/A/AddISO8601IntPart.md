# AddISO8601IntPart

## Location
[src/interfaces/ecpg/pgtypeslib/interval.c:723-732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/interval.c#L723-L732)

## Overview
Appends an ISO 8601-style interval field component to an output string, but only if the value is non-zero, following the ISO 8601 interval format with single-character unit designators.

## Definition
```c
static char *AddISO8601IntPart(char *cp, int64 value, char units)
```

## Detailed Description
This helper function formats individual interval components for ISO 8601 interval output format. The ISO 8601 standard specifies interval notation using single-character designators (Y, M, W, D for date components; H, M, S for time components) immediately following numeric values, without spaces.

The function provides the simplest formatting among the interval helper functions:
1. **Zero suppression**: Returns immediately without output if the value is zero
2. **Compact format**: No spaces, signs, or pluralization - just the numeric value followed by the unit character
3. **Direct output**: Straightforward sprintf formatting without conditional logic

This produces output components like "2Y", "6M", "7D", "1H", "30M", "45S" that combine to form complete ISO 8601 intervals such as "P2Y6M7DT1H30M45S".

## Parameters / Member Variables
- `cp`: Current position in the output string buffer where text should be appended
- `value`: The numeric value for this interval component (should be positive in ISO 8601 format)
- `units`: Single character unit designator ('Y', 'M', 'W', 'D', 'H', 'M', 'S')

## Dependencies
- Functions called/Symbols referenced:
  - sprintf - formats the output string with simple numeric and character formatting
  - strlen - calculates string length for advancing the buffer pointer
- Called from:
  - [EncodeInterval](../E/EncodeInterval.md) (src/backend/utils/adt/datetime.c:4694-4696, 4699-4700) - for all date and time components
  - [EncodeInterval](../E/EncodeInterval.md) (src/interfaces/ecpg/pgtypeslib/interval.c:863-865, 868-869) - ECPG library version

## Notes and Other Information
- Returns an updated buffer pointer positioned after the newly appended text
- The function is static, so it's only used within the same source file
- This is the simplest of the three interval formatting helper functions (ISO8601, Postgres, Verbose)
- ISO 8601 format assumes positive values; sign handling is typically done at the interval level with a leading minus sign
- No pluralization is needed since ISO 8601 uses single-character unit designators
- The format produces compact, standardized output suitable for machine processing and international interchange