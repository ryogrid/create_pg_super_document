# add_pos

## Location
src/backend/utils/adt/tsvector_op.c: 364 - 399

## Overview
A static function that adds positions from a source TSVector word entry to a destination TSVector word entry, offsetting the positions by a maximum position value.

## Definition


## Detailed Description
The  function copies word position data from a source TSVector word entry to a destination word entry, applying a position offset. It's designed to handle TSVector concatenation operations where position values need to be adjusted to maintain proper ordering. The function includes overflow protection and position limits to ensure data integrity.

The function processes position data by:
1. Extracting position arrays from both source and destination entries
2. Iterating through source positions while checking various constraints
3. Copying each position with weight information, applying the offset
4. Updating position counts and setting the haspos flag

## Parameters / Member Variables
- : Source TSVector containing the positions to copy
- : Pointer to the WordEntry in the source TSVector
- : Destination TSVector where positions will be added
- : Pointer to the WordEntry in the destination TSVector
- : Position offset to add to each source position

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