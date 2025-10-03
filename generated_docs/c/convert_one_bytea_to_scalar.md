# convert_one_bytea_to_scalar

## Location
[src/backend/utils/adt/selfuncs.c:4787-4829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L4787-L4829)

## Overview
Converts a single bytea (binary data) value to a normalized scalar value between 0 and 1 using a fractional representation based on byte values, specifically designed for non-null-terminated binary data.

## Definition

```c
static double
convert_one_bytea_to_scalar(unsigned char *value, int valuelen,
							int rangelo, int rangehi)
```
## Detailed Description
This function performs the mathematical conversion of binary data to a scalar value for selectivity estimation in PostgreSQL's query planner. It operates similarly to  but is specifically optimized for binary data handling.

Key characteristics:
1. **Length-based Processing**: Unlike string conversion, it takes an explicit length parameter since binary data is not null-terminated.

2. **Shorter Processing Limit**: Processes at most 10 bytes compared to 12 characters for strings, since the base is fixed at 256 (full byte range), providing sufficient precision with fewer bytes.

3. **Fixed Base System**: Uses the full byte range (rangelo to rangehi, typically 0-255) as the base for the fractional conversion, treating each byte as a digit in base-256.

4. **Fractional Representation**: Converts the binary data using the formula: byte[0]/base + byte[1]/base² + byte[2]/base³ + ...

The function handles edge cases by clamping out-of-range values and returns 0.0 for empty data, ensuring consistent behavior across all inputs.

## Parameters / Member Variables
- `*value`: Pointer to the unsigned char array containing the binary data
- `valuelen`: The length of the binary data in bytes
- `rangelo`: The lowest byte value in the expected range (typically 0)
- `rangehi`: The highest byte value in the expected range (typically 255)
## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - operates directly on byte data)
- Called from (representative examples):
  - [convert_bytea_to_scalar](convert_bytea_to_scalar.md) (called 3 times for value, lobound, and hibound conversion)

## Notes and Other Information
- Returns 0.0 for empty binary data (valuelen <= 0), providing a consistent baseline
- The 10-byte limit is optimized for binary data since base 256 provides more information density than variable-base string conversion
- Out-of-range bytes are clamped to boundary values (rangelo-1 or rangehi+1) to preserve ordering relationships
- The function is static, indicating it's an internal helper for binary-to-scalar conversion
- Uses unsigned char to ensure proper handling of byte values 0-255 without sign extension issues
- Part of PostgreSQL's selectivity estimation system, specifically designed for bytea column statistics
- The fixed-base approach is simpler than the string version's dynamic range analysis, reflecting the nature of binary data
- Critical for accurate query planning when dealing with bytea columns in WHERE clauses and JOIN conditions

## Simplified Source

```c
static double convert_one_bytea_to_scalar(unsigned char *value, int valuelen,
                                          int rangelo, int rangehi) {
    if (valuelen <= 0)
        return 0.0; // Empty data = 0

    // Limit processing to 10 bytes for efficiency
    if (valuelen > 10)
        valuelen = 10;

    // Convert bytes to fractional value
    double base = rangehi - rangelo + 1;
    double num = 0.0;
    double denom = base;

    while (valuelen-- > 0) {
        int ch = *value++;

        // Clamp out-of-range bytes to preserve ordering
        if (ch < rangelo) ch = rangelo - 1;
        else if (ch > rangehi) ch = rangehi + 1;

        // Add fractional component: byte/base + byte/base² + ...
        num += ((double) (ch - rangelo)) / denom;
        denom *= base;
    }

    return num;
}
```