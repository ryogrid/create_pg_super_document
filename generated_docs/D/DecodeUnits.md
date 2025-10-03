# DecodeUnits

## Location
[src/interfaces/ecpg/pgtypeslib/dt_common.c:536-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/dt_common.c#L536-L580)

## Overview
A cached lookup function that decodes text strings representing time interval units using a lookup table, with performance optimization through caching.

## Definition

```c
int
DecodeUnits(int field, char *lowtoken, int *val)
```
## Detailed Description
DecodeUnits recognizes keywords associated with time interval units by performing lookups in a pre-built token table. The function implements a cache mechanism (deltacache) to optimize performance based on the assumption that dates will often be related in format, making cache hits likely. 

The function first checks if there's a cached entry for the given field, and if the cached token matches the input. If not, it performs a binary search using datebsearch() on the deltatktbl table. When a match is found, the result is cached for future use. The input string must already be lowercased before calling this function.

## Parameters / Member Variables
- `field`: Index for the cache array to store/retrieve cached tokens
- `*lowtoken`: The lowercased string token representing a time unit to decode
- `*val`: Output parameter that receives the decoded numeric value of the time unit
## Dependencies
- Functions called/Symbols referenced:
  - datetkn (structure type for date/time tokens)
  - TOKMAXLEN (maximum token length constant)
  - [datebsearch](../d/datebsearch.md) (binary search function for date tokens)
  - UNKNOWN_FIELD (constant returned when token is not recognized)
  - deltacache (cache array for storing recent lookups)
  - deltatktbl (main lookup table for time interval tokens)
  - szdeltatktbl (size of the deltatktbl array)
- Called from (representative examples):
  - [extract_date](../e/extract_date.md)
  - [time_part_common](../t/time_part_common.md)
  - [DecodeInterval](DecodeInterval.md)
  - [timestamp_trunc](../t/timestamp_trunc.md)
  - [interval_part_common](../i/interval_part_common.md)

## Notes and Other Information
- Returns the type of the decoded unit (from the datetkn structure) or UNKNOWN_FIELD if not found
- The caching mechanism significantly improves performance for repetitive date/time operations
- Input token must be pre-lowercased - the function does not perform case conversion
- Uses strncmp with TOKMAXLEN to support truncated token matching
- The val parameter is set to 0 if the token is not recognized
- This function is crucial for parsing interval expressions and date/time arithmetic operations

## Simplified Source

```c
int
DecodeUnits(int field, const char *lowtoken, int *val)
{
    int type;
    const datetkn *tp;

    // First try cache lookup for performance
    tp = deltacache[field];
    if (tp == NULL || strncmp(lowtoken, tp->token, TOKMAXLEN) != 0)
    {
        // Cache miss - search the main token table
        tp = datebsearch(lowtoken, deltatktbl, szdeltatktbl);
    }

    if (tp == NULL)
    {
        // Token not found
        type = UNKNOWN_FIELD;
        *val = 0;
    }
    else
    {
        // Found - cache result and return values
        deltacache[field] = tp;
        type = tp->type;
        *val = tp->value;
    }

    return type;
}
```