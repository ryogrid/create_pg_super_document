# tsvector_concat

## Location
src/backend/utils/adt/tsvector_op.c: 925 - 1151

## Overview
Concatenates two tsvectors by merging their lexemes in sorted order, combining duplicate entries and adjusting position offsets to maintain proper position sequencing across the concatenated result.

## Definition


## Detailed Description
The  function implements the concatenation operation for PostgreSQL tsvectors (typically used via the  operator). It performs a sorted merge of two input tsvectors, creating a new tsvector that contains all unique lexemes from both inputs. When duplicate lexemes are found, their positional information is merged. 

The function maintains proper position sequencing by finding the maximum position in the first tsvector and using it as an offset when adding positions from the second tsvector. This ensures that positions from the second tsvector appear after those from the first, preserving the logical document order.

The implementation uses a three-way merge algorithm similar to merging sorted arrays, handling cases where lexemes appear in only the first tsvector, only the second, or in both. Memory allocation is conservative initially, then compacted at the end to minimize space usage.

## Parameters / Member Variables
- **PG_FUNCTION_ARGS**: Standard PostgreSQL function argument structure containing:
  - Argument 0: First tsvector input
  - Argument 1: Second tsvector input

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR: Extract tsvector arguments from function call
  - ARRPTR: Get pointer to WordEntry array in tsvector
  - STRPTR: Get pointer to string data in tsvector
  - POSDATALEN: Get length of position data for a word entry
  - POSDATAPTR: Get pointer to position data for a word entry
  - WEP_GETPOS: Extract position from WordEntryPos
  - VARSIZE: Get variable-length type size
  - SET_VARSIZE: Set variable-length type size
  - compareEntry: Compare two word entries lexicographically
  - _POSVECPTR: Get pointer to position vector for a word entry
  - SHORTALIGN: Align memory addresses to short boundaries
  - add_pos: Helper function to add positions with offset
  - MAXSTRPOS: Maximum allowed string position
  - CALCDATASIZE: Calculate total data size for tsvector
  - PG_RETURN_POINTER: Return result pointer
  - PG_FREE_IF_COPY: Free copied input arguments

- Called from (representative examples):
  - No direct callers found (exposed as PostgreSQL SQL operator/function)

## Notes and Other Information
- The function implements the PostgreSQL  concatenation operator for tsvectors
- Position offsets are calculated to maintain proper document order across concatenation
- Lexemes are kept in sorted order in the result tsvector
- When duplicate lexemes exist, their positions are merged (union of positions)
- Memory is initially over-allocated conservatively, then compacted to actual size
- Position overflow checking ensures results don't exceed MAXSTRPOS limits
- The  helper function handles position offset arithmetic and overflow detection
- Handles cases where either input tsvector may be empty
- Maintains the haspos flags correctly for lexemes with and without positional data
- Uses SHORTALIGN for proper memory alignment of position data structures