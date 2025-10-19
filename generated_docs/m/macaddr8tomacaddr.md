# macaddr8tomacaddr

## Location
[src/backend/utils/adt/mac8.c:545-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L545-L569)

## Overview
Converts an 8-byte MAC address (macaddr8) to a 6-byte MAC address (macaddr) by removing the EUI-64 expansion bytes, with validation to ensure proper format.

## Definition
```c
Datum macaddr8tomacaddr(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the conversion from an 8-byte MAC address (macaddr8 type) back to a 6-byte MAC address (macaddr type) by reversing the EUI-64 expansion. The function validates that the input macaddr8 contains the standard EUI-64 expansion bytes (0xFF and 0xFE) at positions d and e before performing the conversion.

The conversion works by:
1. Validating that bytes d and e contain 0xFF and 0xFE respectively
2. If validation fails, throwing an error with detailed explanation
3. If valid, extracting bytes a, b, c, f, g, h to form the 6-byte result

The conversion follows this mapping:
- Input: aa:bb:cc:ff:fe:dd:ee:ff
- Result: aa:bb:cc:dd:ee:ff

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_MACADDR8_P(0)`: The 8-byte MAC address to convert

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MACADDR8_P`: Retrieves the input macaddr8 parameter
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory for the result
  - `ereport`: Reports error when conversion is not possible
  - `PG_RETURN_MACADDR_P`: Returns the macaddr result
- Types referenced:
  - `[macaddr8](macaddr8.md)`: Input 8-byte MAC address type
  - `[macaddr](macaddr.md)`: Output 6-byte MAC address type
- Called from (representative examples):
  - No direct references found in the analyzed code

## Notes and Other Information
- The function includes strict validation - only macaddr8 values with 0xFF and 0xFE at positions d and e can be converted
- Error reporting includes a helpful hint explaining the required format: "xx:xx:xx:ff:fe:xx:xx:xx"
- Uses `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` error code for invalid conversions
- This is the complementary function to `macaddrtomacaddr8` which performs the reverse conversion
- Memory allocation uses `palloc0` to ensure the result structure is zero-initialized
- Located in src/backend/utils/adt/mac8.c:545-569

## Simplified Source

```c
macaddr* macaddr8tomacaddr(macaddr8 *addr) {
    // Convert 8-byte MAC to 6-byte MAC by removing EUI-64 expansion bytes

    // Validate that EUI-64 expansion bytes are present
    if ((addr->d != 0xFF) || (addr->e != 0xFE)) {
        error("Invalid macaddr8 format - missing FF:FE expansion bytes");
    }

    macaddr *result = allocate_macaddr();

    // Extract original 6-byte MAC address
    result->a = addr->a;  // First 3 bytes unchanged
    result->b = addr->b;
    result->c = addr->c;
    result->d = addr->f;  // Skip expansion bytes, take last 3 bytes
    result->e = addr->g;
    result->f = addr->h;

    return result;
}
```