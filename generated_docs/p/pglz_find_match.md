# pglz_find_match

## Location
[src/common/pg_lzcompress.c:399-508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/pg_lzcompress.c#L399-L508)

## Overview
Searches the compression history table to find matching character sequences that have occurred earlier in the input buffer, enabling efficient LZ compression by identifying repeating patterns.

## Definition
```c
static inline int pglz_find_match(int16 *hstart, const char *input, const char *end,
                                  int *lenp, int *offp, int good_match, int good_drop, int mask)
```

## Detailed Description
This function implements the core pattern-matching logic for PostgreSQL's LZ compression algorithm. It traverses a linked history list to find the longest matching sequence between the current input position and previously seen data. The function uses an optimized approach where it accepts progressively shorter matches as it examines more history entries, balancing compression effectiveness with performance.

The algorithm employs several optimizations:
- Uses memcmp() for matches of 16+ bytes for better performance
- Implements adaptive matching where acceptable match length decreases as search progresses
- Limits offset to 12-bit range (0x0fff) to fit encoding constraints
- Only returns matches that provide at least 1 byte of compression benefit

## Parameters
- `hstart`: Array of hash table start indices for history lookup
- `input`: Current position in input buffer to find matches for  
- `end`: End boundary of input buffer
- `lenp`: Output parameter for match length
- `offp`: Output parameter for match offset (distance back to matched sequence)
- `good_match`: Initial threshold for acceptable match length
- `good_drop`: Percentage to reduce good_match threshold per history entry examined
- `mask`: Hash table size mask (table_size - 1) for hash calculations

## Dependencies
- Functions called/Symbols referenced:
  - [PGLZ_HistEntry](../P/PGLZ_HistEntry.md) (history entry structure)
  - pglz_hist_idx (hash function for history table indexing)
  - INVALID_ENTRY_PTR (sentinel value for end of history chain)
  - PGLZ_MAX_MATCH (maximum allowed match length constant)
- Called from:
  - [pglz_compress](pglz_compress.md) (main compression function)

## Notes and Other Information
- Returns 1 if a beneficial match is found (length > 2), 0 otherwise
- Match offset is limited to 12 bits (4095 bytes) to fit the encoding format
- Uses progressive match quality degradation to balance speed vs compression ratio
- Critical performance path in PostgreSQL's compression system
- Inline function for optimal performance in compression loops

## Simplified Source

```c
static inline int pglz_find_match(int16 *hstart, const char *input, const char *end,
                                  int *lenp, int *offp, int good_match, int good_drop, int mask)
{
    PGLZ_HistEntry *hent;
    int16 hentno;
    int32 len = 0;
    int32 off = 0;

    // Start from hash table entry for current input position
    hentno = hstart[pglz_hist_idx(input, end, mask)];
    hent = &hist_entries[hentno];

    // Traverse history list looking for matches
    while (hent != INVALID_ENTRY_PTR)
    {
        const char *ip = input;
        const char *hp = hent->pos;
        int32 thisoff;
        int32 thislen;

        // Check if offset fits in encoding (12 bits max)
        thisoff = ip - hp;
        if (thisoff >= 0x0fff)
            break;

        // Find length of match at this position
        thislen = 0;

        // For long existing matches, use memcmp for efficiency
        if (len >= 16)
        {
            if (memcmp(ip, hp, len) == 0)
            {
                thislen = len;
                ip += len;
                hp += len;

                // Extend match character by character
                while (ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH)
                {
                    thislen++;
                    ip++;
                    hp++;
                }
            }
        }
        else
        {
            // For short matches, compare character by character
            while (ip < end && *ip == *hp && thislen < PGLZ_MAX_MATCH)
            {
                thislen++;
                ip++;
                hp++;
            }
        }

        // Update best match if this one is better
        if (thislen > len)
        {
            len = thislen;
            off = thisoff;
        }

        // Move to next history entry
        hent = hent->next;

        // Use progressively lower match standards
        if (hent != INVALID_ENTRY_PTR)
        {
            if (len >= good_match)
                break;
            good_match -= (good_match * good_drop) / 100;
        }
    }

    // Return match only if it saves at least 1 byte
    if (len > 2)
    {
        *lenp = len;
        *offp = off;
        return 1;
    }

    return 0;
}
```