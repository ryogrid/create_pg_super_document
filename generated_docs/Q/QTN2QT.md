# QTN2QT

## Location
src/backend/utils/adt/tsquery_util.c: 363 - 395

## Overview
Converts a QTNode tree representation into a flat TSQuery structure for efficient storage and processing.

## Definition


## Detailed Description
QTN2QT transforms a tree-based query representation (QTNode) into a flattened TSQuery format. This conversion is essential for tsquery operations as it creates the compact binary representation used throughout PostgreSQL's text search system. The function performs size calculations, validates query limits, allocates memory for the result, and then fills the flat structure using a state-based approach.

## Parameters / Member Variables
- : QTNode tree structure representing the parsed tsquery that needs to be converted to flat format

## Dependencies
- Functions called/Symbols referenced:
  - cntsize (calculates total size and node count)
  - TSQUERY_TOO_BIG (macro to check size limits)
  - COMPUTESIZE (calculates required memory size)
  - palloc0 (allocates zero-initialized memory)
  - SET_VARSIZE (sets variable-length structure size)
  - GETQUERY (gets query item array from TSQuery)
  - GETOPERAND (gets operand data from TSQuery)
  - fillQT (fills the flat structure from tree)
- Called from (representative examples):
  - tsquery_and (logical AND operations)
  - tsquery_or (logical OR operations)
  - tsquery_phrase_distance (phrase distance operations)
  - tsquery_not (logical NOT operations)
  - tsquery_rewrite_query (query rewriting)

## Notes and Other Information
- Raises ERROR with ERRCODE_PROGRAM_LIMIT_EXCEEDED if the tsquery exceeds size limits
- Uses QTN2QTState structure to track current position during flat structure creation
- The resulting TSQuery uses a compact binary format for efficient storage and processing
- Critical function in tsquery processing pipeline, converting parsed trees to executable format