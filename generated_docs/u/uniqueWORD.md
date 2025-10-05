# uniqueWORD

## Location
[src/backend/tsearch/to_tsany.c:77-164](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L77-L164)

## Overview
A static function that sorts an array of ParsedWord structures, removes duplicates, and consolidates position information for identical words into position arrays.

## Definition

```c
static int
uniqueWORD(ParsedWord *a, int32 l)
```
## Detailed Description
 performs deduplication and position consolidation on an array of ParsedWord structures. The function implements the following algorithm:

1. **Special case handling**: For single-word arrays, it simply converts the position to an array format and returns.

2. **Sorting**: Uses  with the  comparison function to sort all words lexicographically, with position as a secondary sort key.

3. **Deduplication and position consolidation**: Iterates through the sorted array and:
   - For unique words: Creates a new entry with its position stored in an array
   - For duplicate words: Consolidates the position information into the existing word's position array, subject to limits

4. **Position array management**: Each unique word maintains a dynamic array of positions where:
   -  stores the count of positions
   -  through  store the actual position values
   - Arrays are grown as needed using 

The function enforces several limits: maximum number of positions per word (), maximum position value (), and ensures position uniqueness within each word's array.

## Parameters / Member Variables
- `*a`: Array of ParsedWord structures to process
- `l`: Length of the input array (number of elements)
## Dependencies
- Functions called/Symbols referenced:
  - : Comparison function for sorting ParsedWord structures
  - : Standard C library sorting function
  - : PostgreSQL memory allocation function
  - : PostgreSQL memory reallocation function
  - : PostgreSQL memory deallocation function
  - : Standard C string comparison function
  - : Macro to limit position values
  - : Maximum number of positions allowed per word
  - : Maximum position value allowed
- Called from (representative examples):
  - : Uses this function to deduplicate words before creating tsvector

## Notes and Other Information
- This is a static function internal to 
- Critical component of tsvector creation, ensuring each word appears only once with all its positions
- The position array format follows PostgreSQL's tsvector internal representation
- Memory management is handled through PostgreSQL's memory context system
- Position limits prevent excessive memory usage and maintain compatibility with tsvector storage format
- Located at lines 77-164 in 
- The function modifies the input array in-place, compacting unique results at the beginning

## Simplified Source

```c
static int uniqueWORD(ParsedWord *a, int32 l) {
    ParsedWord *current, *result;
    int position;

    // Handle single word case
    if (l == 1) {
        position = LIMITPOS(a->pos.pos);
        a->alen = 2;
        a->pos.apos = (uint16 *) palloc(sizeof(uint16) * a->alen);
        a->pos.apos[0] = 1;  // position count
        a->pos.apos[1] = position;  // first position
        return l;
    }

    // Sort words by content and position
    qsort(a, l, sizeof(ParsedWord), compareWORD);

    // Initialize first word's position array
    result = a;
    current = a + 1;
    position = LIMITPOS(a->pos.pos);
    a->alen = 2;
    a->pos.apos = (uint16 *) palloc(sizeof(uint16) * a->alen);
    a->pos.apos[0] = 1;
    a->pos.apos[1] = position;

    // Process remaining words
    while (current - a < l) {
        if (!(current->len == result->len &&
              strncmp(current->word, result->word, result->len) == 0)) {
            // New unique word - add to result
            result++;
            result->len = current->len;
            result->word = current->word;
            position = LIMITPOS(current->pos.pos);
            result->alen = 2;
            result->pos.apos = (uint16 *) palloc(sizeof(uint16) * result->alen);
            result->pos.apos[0] = 1;
            result->pos.apos[1] = position;
        } else {
            // Duplicate word - merge position if within limits
            pfree(current->word);
            if (result->pos.apos[0] < MAXNUMPOS - 1 &&
                result->pos.apos[result->pos.apos[0]] != MAXENTRYPOS - 1 &&
                result->pos.apos[result->pos.apos[0]] != LIMITPOS(current->pos.pos)) {

                // Expand position array if needed
                if (result->pos.apos[0] + 1 >= result->alen) {
                    result->alen *= 2;
                    result->pos.apos = (uint16 *) repalloc(result->pos.apos,
                                                          sizeof(uint16) * result->alen);
                }

                // Add new position if unique
                if (result->pos.apos[0] == 0 ||
                    result->pos.apos[result->pos.apos[0]] != LIMITPOS(current->pos.pos)) {
                    result->pos.apos[result->pos.apos[0] + 1] = LIMITPOS(current->pos.pos);
                    result->pos.apos[0]++;
                }
            }
        }
        current++;
    }

    return result + 1 - a;  // return number of unique words
}
```