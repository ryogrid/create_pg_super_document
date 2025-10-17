# uniquePos

## Location
[src/backend/utils/adt/tsvector.c:52-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L52-L86)

## Overview
Removes duplicate position entries from a WordEntryPos array while preserving the highest weight for each position.

## Definition
```c
static int uniquePos(WordEntryPos *a, int l)
```

## Detailed Description
This function processes an array of WordEntryPos structures to eliminate duplicates based on position values. Unlike a simple unique operation, it implements intelligent duplicate handling by preserving the entry with the highest weight when multiple entries share the same position. The function first sorts the array using compareWordEntryPos, then performs a single pass to remove duplicates while maintaining weight precedence. It also enforces limits on the maximum number of positions (MAXNUMPOS) and maximum entry position value (MAXENTRYPOS) to prevent overflow conditions.

## Parameters / Member Variables
- `a`: Array of WordEntryPos structures to process for duplicates
- `l`: Length of the input array (number of WordEntryPos elements)

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard library sorting function)
  - [compareWordEntryPos](../c/compareWordEntryPos.md) (comparator for WordEntryPos sorting)
  - WEP_GETPOS (macro to extract position from WordEntryPos)
  - WEP_GETWEIGHT (macro to extract weight from WordEntryPos)
  - WEP_SETWEIGHT (macro to set weight in WordEntryPos)
  - MAXNUMPOS (maximum number of positions allowed)
  - MAXENTRYPOS (maximum position value allowed)
- Called from (representative examples):
  - [uniqueentry](uniqueentry.md) (during tsvector entry processing)

## Notes and Other Information
- Returns the new length of the array after duplicate removal
- Modifies the input array in-place for memory efficiency
- Handles edge case where array length is 1 or less by returning immediately
- Implements early termination when reaching maximum position limits
- Critical for maintaining tsvector data integrity in PostgreSQL's full-text search
- The weight preservation logic ensures that more important word occurrences are retained

## Simplified Source

```c
static int
uniquePos(WordEntryPos *a, int l)
{
    WordEntryPos *ptr, *res;

    if (l <= 1)
        return l;

    // Sort positions for deduplication
    qsort(a, l, sizeof(WordEntryPos), compareWordEntryPos);

    // Remove duplicates, keeping higher weights
    res = a;
    ptr = a + 1;
    while (ptr - a < l) {
        if (WEP_GETPOS(*ptr) != WEP_GETPOS(*res)) {
            // Different position - advance result pointer
            res++;
            *res = *ptr;
            // Check limits to prevent overflow
            if (res - a >= MAXNUMPOS - 1 ||
                WEP_GETPOS(*res) == MAXENTRYPOS - 1)
                break;
        } else if (WEP_GETWEIGHT(*ptr) > WEP_GETWEIGHT(*res)) {
            // Same position but higher weight - update weight
            WEP_SETWEIGHT(*res, WEP_GETWEIGHT(*ptr));
        }
        ptr++;
    }

    return res + 1 - a;
}
```