# BinarySearchRange

## Location
[src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c:207-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/conversion_procs/euc_tw_and_big5/big5.c#L207-L291)

## Overview
BinarySearchRange is a static function that performs binary search on a sorted array of character code mappings to find the appropriate conversion between Big5 and CNS 11643-1992 character encodings.

## Definition

```c
};

static unsigned short BinarySearchRange
			(const codes_t *array, int high, unsigned short code)
```
## Detailed Description
This function implements a binary search algorithm specifically designed for character code conversion between Big5 (Traditional Chinese) and CNS 11643-1992 (Chinese National Standard) encodings. The function searches through a sorted array of code mappings to find the range containing the input code and then calculates the corresponding converted code point.

The function handles two conversion directions:
1. **Big5 to CNS**: For codes >= 0xa140U, it performs Big5 to CNS conversion using Big5's unique radix system (0x9d) and handles the two-region structure of Big5 encoding
2. **CNS to Big5**: For codes < 0xa140U, it performs CNS to Big5 conversion using ISO charset's radix system (0x5e)

The algorithm includes sophisticated distance calculations that account for the different encoding structures and byte ranges of each character set.

## Parameters / Member Variables
- `*array`: Pointer to a sorted array of codes_t structures containing code mappings
- `high`: The upper bound index for the binary search (array size - 1)
- `code`: The input character code to be converted
## Dependencies
- Functions called/Symbols referenced:
  - codes_t (structure type for code mappings)
- Called from (representative examples):
  - [BIG5toCNS](BIG5toCNS.md) (at line 310 and 331)
  - [CNStoBIG5](../C/CNStoBIG5.md) (at line 355 and 358)

## Notes and Other Information
- The function uses complex mathematical calculations to handle the different radix systems of Big5 (0x9d) and CNS (0x5e)
- Big5 encoding has two distinct byte regions with a bias adjustment (-0x22) between them
- Returns 0 if no valid mapping is found or if the peer value is 0
- The algorithm assumes the input array is properly sorted by the code field
- Critical for proper character encoding conversion in PostgreSQL's multi-byte character support

## Simplified Source

```c
static unsigned short BinarySearchRange(const codes_t *array, int high, unsigned short code)
{
    int low = 0;
    int mid = high >> 1;

    // Binary search for the code range
    for (; low <= high; mid = (low + high) >> 1)
    {
        if ((array[mid].code <= code) && (array[mid + 1].code > code))
        {
            if (array[mid].peer == 0)
                return 0;

            if (code >= 0xa140U)
            {
                // Big5 to CNS conversion
                int tmp = ((code & 0xff00) - (array[mid].code & 0xff00)) >> 8;
                int high_byte = code & 0x00ff;
                int low_byte = array[mid].code & 0x00ff;

                // Calculate distance with Big5 radix (0x9d) and bias adjustment
                int distance = tmp * 0x9d + high_byte - low_byte +
                    (high_byte >= 0xa1 ? (low_byte >= 0xa1 ? 0 : -0x22)
                     : (low_byte >= 0xa1 ? +0x22 : 0));

                // Convert distance to CNS code point
                tmp = (array[mid].peer & 0x00ff) + distance - 0x21;
                tmp = (array[mid].peer & 0xff00) + ((tmp / 0x5e) << 8)
                    + 0x21 + tmp % 0x5e;
                return tmp;
            }
            else
            {
                // CNS to Big5 conversion
                int tmp = ((code & 0xff00) - (array[mid].code & 0xff00)) >> 8;

                // Calculate distance with ISO charset radix (0x5e)
                int distance = tmp * 0x5e
                    + ((int) (code & 0x00ff) - (int) (array[mid].code & 0x00ff));

                // Convert distance to Big5 code point
                int low_byte = array[mid].peer & 0x00ff;
                tmp = low_byte + distance - (low_byte >= 0xa1 ? 0x62 : 0x40);
                low_byte = tmp % 0x9d;
                tmp = (array[mid].peer & 0xff00) + ((tmp / 0x9d) << 8)
                    + (low_byte > 0x3e ? 0x62 : 0x40) + low_byte;
                return tmp;
            }
        }
        else if (array[mid].code > code)
            high = mid - 1;
        else
            low = mid + 1;
    }

    return 0;  // No mapping found
}
```