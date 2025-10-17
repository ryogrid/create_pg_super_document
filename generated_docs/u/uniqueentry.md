# uniqueentry

## Location
[src/backend/utils/adt/tsvector.c:103-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector.c#L103-L174)

## Overview
Sorts an array of WordEntryIN structures, removes duplicate entries, and merges their positional information while calculating required buffer space.

## Definition
```c
static int uniqueentry(WordEntryIN *a, int l, char *buf, int *outbuflen)
```

## Detailed Description
This function processes an array of WordEntryIN structures to eliminate duplicates based on string content while intelligently merging positional information from duplicate entries. It first sorts the array using compareentry, then performs a single pass to identify and merge duplicates. When duplicate entries are found (same string content), their positional information is consolidated - if both entries have positions, they are merged into a single array; if only one has positions, those positions are preserved. The function also calculates the total buffer space needed for the final data structure, including proper alignment for position data. Each entry's positions are further processed through uniquePos to remove duplicate positions within individual entries.

## Parameters / Member Variables
- `a`: Array of WordEntryIN structures to process for duplicates
- `l`: Length of the input array (number of WordEntryIN elements)
- `buf`: Buffer containing the actual string data referenced by entries
- `outbuflen`: Pointer to receive the calculated buffer space needed for output

## Dependencies
- Functions called/Symbols referenced:
  - qsort_arg (sorting function with additional argument)
  - [compareentry](../c/compareentry.md) (comparator for WordEntryIN sorting)
  - [uniquePos](uniquePos.md) (removes duplicate positions within entries)
  - strncmp (string comparison function)
  - memcpy (memory copy function)
  - [repalloc](../r/repalloc.md) (PostgreSQL memory reallocation function)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - SHORTALIGN (alignment macro)
  - WordEntryIN (input structure type)
  - WordEntryPos (position structure type)
- Called from (representative examples):
  - [tsvectorin](../t/tsvectorin.md) (during tsvector input processing)

## Notes and Other Information
- Returns the new length of the array after duplicate removal
- Modifies the input array in-place for memory efficiency
- Handles complex position merging logic for duplicate entries
- Calculates precise buffer space requirements including alignment
- Critical for tsvector construction from text input in PostgreSQL's full-text search
- Ensures that all positions for identical words are consolidated into single entries
- The function maintains referential integrity between entries and their string data

## Simplified Source

```c
static int
uniqueentry(WordEntryIN *a, int l, char *buf, int *outbuflen)
{
    int buflen;
    WordEntryIN *ptr, *res;

    Assert(l >= 1);

    // Sort entries by string content
    if (l > 1)
        qsort_arg(a, l, sizeof(WordEntryIN), compareentry, buf);

    buflen = 0;
    res = a;
    ptr = a + 1;

    while (ptr - a < l) {
        // Check if current entry differs from previous
        if (!(ptr->entry.len == res->entry.len &&
              strncmp(&buf[ptr->entry.pos], &buf[res->entry.pos],
                      res->entry.len) == 0)) {
            // Different entry - finalize current and advance
            buflen += res->entry.len;
            if (res->entry.haspos) {
                res->poslen = uniquePos(res->pos, res->poslen);
                buflen = SHORTALIGN(buflen);
                buflen += res->poslen * sizeof(WordEntryPos) + sizeof(uint16);
            }
            res++;
            if (res != ptr)
                memcpy(res, ptr, sizeof(WordEntryIN));
        } else if (ptr->entry.haspos) {
            // Same entry with positions - merge position data
            if (res->entry.haspos) {
                // Append ptr's positions to res's positions
                int newlen = ptr->poslen + res->poslen;
                res->pos = repalloc(res->pos, newlen * sizeof(WordEntryPos));
                memcpy(&res->pos[res->poslen], ptr->pos,
                       ptr->poslen * sizeof(WordEntryPos));
                res->poslen = newlen;
                pfree(ptr->pos);
            } else {
                // Transfer ptr's positions to res
                res->entry.haspos = 1;
                res->pos = ptr->pos;
                res->poslen = ptr->poslen;
            }
        }
        ptr++;
    }

    // Process final entry
    buflen += res->entry.len;
    if (res->entry.haspos) {
        res->poslen = uniquePos(res->pos, res->poslen);
        buflen = SHORTALIGN(buflen);
        buflen += res->poslen * sizeof(WordEntryPos) + sizeof(uint16);
    }

    *outbuflen = buflen;
    return res + 1 - a;
}
```