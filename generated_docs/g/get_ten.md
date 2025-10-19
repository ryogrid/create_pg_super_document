# get_ten

## Location
[src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:222-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c#L222-L253)

## Overview
Helper function that decodes the "ten" (column) value and "ku" (row) parity from a Shift-JIS-2004 second byte.

## Definition
```c
static int get_ten(int b, int *ku)
```

## Detailed Description
This utility function analyzes the second byte of a Shift-JIS-2004 character encoding to extract the "ten" (column) position and determine whether the "ku" (row) is even or odd. The function implements the Shift-JIS-2004 encoding rules by examining byte value ranges and calculating the corresponding ten value while setting a ku parity indicator. This is crucial for the reverse conversion process from Shift-JIS-2004 to EUC-JIS-2004.

## Parameters / Member Variables
- `b`: The second byte of a Shift-JIS-2004 character sequence
- `ku`: Pointer to integer that will be set to indicate ku parity (0 = even ku, 1 = odd ku)

## Dependencies
- Functions called/Symbols referenced: None (pure computation)
- Called from:
  - [shift_jis_20042euc_jis_2004](../s/shift_jis_20042euc_jis_2004.md) (multiple times at lines 314, 327, 341, 368)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:222-253
- Static function - only accessible within the same compilation unit
- Returns the ten (column) value, or -1 if the byte is invalid
- Handles three distinct byte ranges:
  - 0x40-0x7E: ten = b - 0x3F, ku = odd (1)
  - 0x80-0x9E: ten = b - 0x40, ku = odd (1)  
  - 0x9F-0xFC: ten = b - 0x9E, ku = even (0)
- Essential component for Shift-JIS-2004 to EUC-JIS-2004 conversion process
- Implements the inverse mapping of the ten encoding used in euc_jis_20042shift_jis_2004

## Simplified Source

```c
static int get_ten(int b, int *ku) {
    // Extract "ten" (column) value from Shift-JIS-2004 second byte
    // and determine "ku" (row) parity

    if (b >= 0x40 && b <= 0x7e) {
        // Range 1: odd ku
        *ku = 1;
        return b - 0x3f;
    }
    else if (b >= 0x80 && b <= 0x9e) {
        // Range 2: odd ku (continuation)
        *ku = 1;
        return b - 0x40;
    }
    else if (b >= 0x9f && b <= 0xfc) {
        // Range 3: even ku
        *ku = 0;
        return b - 0x9e;
    }
    else {
        // Invalid byte value
        *ku = 0;
        return -1;
    }
}
```