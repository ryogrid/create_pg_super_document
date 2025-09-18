# spg_text_picksplit

## Location
[src/backend/access/spgist/spgtextproc.c:333-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L333-L425)

## Overview
The picksplit function for SP-GiST text operator class that partitions a set of text values by finding their longest common prefix and creating child nodes based on the first differing character.

## Definition
```c
Datum spg_text_picksplit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the core partitioning logic for SP-GiST (Space-Partitioned Generalized Search Tree) indexes on text data types. When an SP-GiST node becomes full and needs to be split, this function analyzes all the text values to find the longest common prefix among them. It then creates child nodes based on the first character that differs after this common prefix. The function handles the complete split process including prefix extraction, node labeling, tuple mapping, and leaf data preparation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `in` (spgPickSplitIn *): Input structure containing the tuples to be split and related metadata
  - `out` (spgPickSplitOut *): Output structure to be filled with split results

## Dependencies
- Functions called/Symbols referenced:
  - [spgPickSplitIn](spgPickSplitIn.md), spgPickSplitOut (SP-GiST framework structures)
  - DatumGetTextPP (text datum extraction)
  - [commonPrefix](../c/commonPrefix.md) (utility function to find common prefix length)
  - [formTextDatum](../f/formTextDatum.md) (creates text datum from raw data)
  - [cmpNodePtr](../c/cmpNodePtr.md) (comparator for sorting node pointers)
  - qsort (standard library sorting function)
  - Int16GetDatum (converts int16 to Datum)
- Called from (representative examples):
  - SP-GiST framework during index splits (no direct references found)

## Notes and Other Information
- The function limits the common prefix length to SPGIST_MAX_PREFIX_LENGTH to ensure inner tuples fit on a page
- Uses character value -1 to represent strings that are entirely covered by the common prefix
- Sorts node pointers by character labels to enable binary search in searchChar operations
- Creates both node labels (characters) and leaf tuple data (remaining text after prefix and label)
- Critical for text indexing performance as it determines how efficiently text searches can navigate the tree structure