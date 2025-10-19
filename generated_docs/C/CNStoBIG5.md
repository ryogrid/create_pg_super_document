# CNStoBIG5

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c:345-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c#L345-L377)

## Overview
CNStoBIG5 is a public function that converts CNS 11643-1992 character codes to their corresponding Big5 encoded characters, handling multiple CNS planes with appropriate conversion strategies for each.

## Definition
unsigned short CNStoBIG5(unsigned short cns, unsigned char lc)

## Detailed Description
This function performs character encoding conversion from CNS 11643-1992 (Chinese National Standard) to Big5 (Traditional Chinese). It handles the reverse conversion of BIG5toCNS, supporting all four CNS planes with different conversion mechanisms for each plane.

The conversion process uses a plane-specific approach:
1. **CNS Plane 1** (LC_CNS11643_1): Uses binary search on cnsPlane1ToBig5Level1 mapping array
2. **CNS Plane 2** (LC_CNS11643_2): Uses binary search on cnsPlane2ToBig5Level2 mapping array  
3. **CNS Plane 3** (LC_CNS11643_3): Linear search through b2c3 lookup table for direct mappings
4. **CNS Plane 4** (LC_CNS11643_4): Linear search through b1c4 lookup table for direct mappings

The function first masks the input CNS code with 0x7f7f to normalize the input, then applies the appropriate conversion strategy based on the locale code parameter.

## Parameters / Member Variables
- : Input CNS 11643-1992 character code to be converted (masked with 0x7f7f internally)
- : CNS plane identifier (LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, or LC_CNS11643_4) indicating which CNS plane the input code belongs to

## Dependencies
- Functions called/Symbols referenced:
  - [BinarySearchRange](../B/BinarySearchRange.md) (called at lines 355 and 358)
  - LC_CNS11643_1, LC_CNS11643_2, LC_CNS11643_3, LC_CNS11643_4 (locale constants)
  - cnsPlane1ToBig5Level1 (mapping array for Plane 1)
  - cnsPlane2ToBig5Level2 (mapping array for Plane 2)
  - b2c3 (lookup table for Plane 3 to Big5 Level 2)
  - b1c4 (lookup table for Plane 4 to Big5 Level 1)
- Called from (representative examples):
  - [euc_tw2big5](../e/euc_tw2big5.md) (in euc_tw_and_big5.c at line 190)
  - [mic2big5](../m/mic2big5.md) (in euc_tw_and_big5.c at line 556)

## Notes and Other Information
- Returns 0 for unsupported planes or when no mapping is found
- Uses direct lookup tables (linear search) for Planes 3 and 4, which typically have fewer mappings
- Uses binary search for Planes 1 and 2, which have larger mapping sets
- The 0x7f7f mask removes the high bit formatting used in CNS encoding
- Essential component of PostgreSQL's bidirectional character encoding support between CNS and Big5
- Complements the BIG5toCNS function to provide complete round-trip conversion capability

## Simplified Source

```c
unsigned short
CNStoBIG5(unsigned short cns, unsigned char lc)
{
    int i;
    unsigned int big5 = 0;

    // Normalize CNS code by removing high bit formatting
    cns &= 0x7f7f;

    switch (lc)
    {
        case LC_CNS11643_1:
            // Use binary search for Plane 1 to Big5 Level 1
            big5 = BinarySearchRange(cnsPlane1ToBig5Level1, 24, cns);
            break;

        case LC_CNS11643_2:
            // Use binary search for Plane 2 to Big5 Level 2
            big5 = BinarySearchRange(cnsPlane2ToBig5Level2, 47, cns);
            break;

        case LC_CNS11643_3:
            // Linear search through Plane 3 lookup table
            for (i = 0; i < sizeof(b2c3) / (sizeof(unsigned short) * 2); i++)
            {
                if (b2c3[i][1] == cns)
                    return b2c3[i][0];
            }
            break;

        case LC_CNS11643_4:
            // Linear search through Plane 4 lookup table
            for (i = 0; i < sizeof(b1c4) / (sizeof(unsigned short) * 2); i++)
            {
                if (b1c4[i][1] == cns)
                    return b1c4[i][0];
            }
            break;

        default:
            break;
    }

    return big5;
}
```