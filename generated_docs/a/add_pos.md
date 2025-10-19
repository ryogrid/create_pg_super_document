# add_pos

## Location
[src/backend/utils/adt/tsvector_op.c:364-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L364-L399)

## Overview
A static function that adds positions from a source TSVector word entry to a destination TSVector word entry, offsetting the positions by a maximum position value.

## Definition

```c
static int32
add_pos(TSVector src, WordEntry *srcptr,
		TSVector dest, WordEntry *destptr,
		int32 maxpos)
```
## Detailed Description
The  function copies word position data from a source TSVector word entry to a destination word entry, applying a position offset. It's designed to handle TSVector concatenation operations where position values need to be adjusted to maintain proper ordering. The function includes overflow protection and position limits to ensure data integrity.

The function processes position data by:
1. Extracting position arrays from both source and destination entries
2. Iterating through source positions while checking various constraints
3. Copying each position with weight information, applying the offset
4. Updating position counts and setting the haspos flag

## Parameters / Member Variables
- `src`: Source TSVector containing the positions to copy
- `*srcptr`: Pointer to the WordEntry in the source TSVector
- `dest`: Destination TSVector where positions will be added
- `*destptr`: Pointer to the WordEntry in the destination TSVector
- `maxpos`: Position offset to add to each source position
## Dependencies
- Functions called/Symbols referenced:
  - _POSVECPTR (macro for position vector access)
  - POSDATALEN (macro to get position data length)
  - POSDATAPTR (macro to get position data pointer)
  - WEP_GETPOS/WEP_SETPOS (macros for position access)
  - WEP_GETWEIGHT/WEP_SETWEIGHT (macros for weight access)
  - LIMITPOS (macro to limit position values)
  - MAXNUMPOS (maximum number of positions constant)
  - MAXENTRYPOS (maximum entry position constant)
- Called from:
  - [tsvector_concat](../t/tsvector_concat.md) (multiple times during concatenation operations)

## Notes and Other Information
- Returns the number of positions actually added (may be less than source due to overflow constraints)
- Includes protection against position overflow by checking MAXNUMPOS and MAXENTRYPOS limits
- Sets the haspos flag on the destination word entry when positions are successfully added
- Uses LIMITPOS to ensure position values don't exceed valid ranges
- Critical for maintaining position data integrity during TSVector concatenation operations

## Simplified Source

```c
static int32 add_pos(TSVector src, WordEntry *srcptr,
                     TSVector dest, WordEntry *destptr,
                     int32 maxpos) {
    uint16 *clen = &_POSVECPTR(dest, destptr)->npos;
    int i;
    uint16 slen = POSDATALEN(src, srcptr), startlen;
    WordEntryPos *spos = POSDATAPTR(src, srcptr);
    WordEntryPos *dpos = POSDATAPTR(dest, destptr);

    // Initialize destination position count if needed
    if (!destptr->haspos)
        *clen = 0;

    startlen = *clen;

    // Copy positions with offset, checking limits
    for (i = 0;
         i < slen && *clen < MAXNUMPOS &&
         (*clen == 0 || WEP_GETPOS(dpos[*clen - 1]) != MAXENTRYPOS - 1);
         i++) {
        // Copy weight and add position offset
        WEP_SETWEIGHT(dpos[*clen], WEP_GETWEIGHT(spos[i]));
        WEP_SETPOS(dpos[*clen], LIMITPOS(WEP_GETPOS(spos[i]) + maxpos));
        (*clen)++;
    }

    // Mark destination as having positions if any were added
    if (*clen != startlen)
        destptr->haspos = 1;

    return *clen - startlen;  // Number of positions added
}
```