# get_docrep

## Location
src/backend/utils/adt/tsrank.c: 727 - 849

## Overview
Constructs a DocRepresentation array from a TSVector and QueryRepresentation, creating a structured representation of document terms that match query operands with their positional information.

## Definition
static DocRepresentation *get_docrep(TSVector txt, QueryRepresentation *qr, int *doclen)

## Detailed Description
This function processes a TSVector document and builds a DocRepresentation array containing all terms that match the query operands. It iterates through each query operand, finds corresponding word entries in the TSVector, extracts positional information, and creates DocRepresentation entries. The function handles weight filtering, memory allocation, sorting, and consolidation of multiple query items at the same position. The resulting array is sorted by position and optimized for subsequent ranking calculations.

## Parameters / Member Variables
- `txt`: TSVector containing the document text with positional information
- `qr`: QueryRepresentation structure containing query operands to match
- `doclen`: Output parameter that receives the length of the returned DocRepresentation array

## Dependencies
- Functions called/Symbols referenced:
  - QueryRepresentation (struct type)
  - TSVector (document vector type)
  - GETQUERY (macro to extract query)
  - QueryItem (query item structure)
  - WordEntry (word entry structure)
  - WordEntryPos (word position structure)
  - DocRepresentation (document representation structure)
  - QueryOperand (query operand structure)
  - QI_VAL (query item value type)
  - [find_wordentry](../f/find_wordentry.md) (find word entries function)
  - POSDATALEN (position data length macro)
  - POSDATAPTR (position data pointer macro)
  - [repalloc](../r/repalloc.md) (memory reallocation function)
  - WEP_GETWEIGHT (extract weight from position)
  - [compareDocR](../c/compareDocR.md) (comparison function for sorting)
  - qsort (standard sorting function)
- Called from (representative examples):
  - [calc_rank_cd](../c/calc_rank_cd.md) (called at line 879)

## Notes and Other Information
This function performs several key optimizations: dynamic memory allocation that grows as needed, weight-based filtering of positions, and consolidation of multiple query items at the same position. The sorting step is crucial for subsequent algorithms like Cover that depend on position-ordered data. The function returns NULL if no matching terms are found and handles memory cleanup appropriately. The consolidation phase groups multiple query items that occur at the same document position into single DocRepresentation entries.