# calc_rank_cd

## Location
src/backend/utils/adt/tsrank.c: 850 - 952

## Overview
Calculates text search ranking using cover density algorithm, evaluating how tightly query terms cluster together in the document with various normalization options.

## Definition
static float4 calc_rank_cd(const float4 *arrdata, TSVector txt, TSQuery query, int method)

## Detailed Description
This function implements the cover density ranking algorithm for PostgreSQL's text search functionality. It builds a document representation, finds all covers (minimal spans containing query terms), and calculates a ranking score based on cover density and proximity. The algorithm considers term weights, cover tightness, and distance between covers. Multiple normalization methods can be applied including document length, unique terms, logarithmic scaling, and extent distribution normalization.

## Parameters / Member Variables
- `arrdata`: Array of weight coefficients for different term categories (A, B, C, D)
- `txt`: TSVector containing the document with positional information
- `query`: TSQuery containing the search query terms and operators
- `method`: Bitmask specifying normalization methods to apply

## Dependencies
- Functions called/Symbols referenced:
  - TSVector (document vector type)
  - TSQuery (query type)
  - float4 (floating point return type)
  - DocRepresentation (document representation structure)
  - CoverExt (cover extension structure)
  - lengthof (array length macro)
  - QueryRepresentation (query representation structure)
  - QueryRepresentationOperand (operand data structure)
  - get_docrep (build document representation)
  - MemSet (memory initialization)
  - Cover (find covers algorithm)
  - WEP_GETWEIGHT (extract term weight)
  - cnt_length (count document length)
  - RANK_NORM_* (normalization method constants)
- Called from (representative examples):
  - ts_rankcd_wttf (called at line 961)
  - ts_rankcd_wtt (called at line 977)
  - ts_rankcd_ttf (called at line 993)
  - ts_rankcd_tt (called at line 1007)

## Notes and Other Information
This function is the core implementation of PostgreSQL's cover density ranking algorithm, which is considered more sophisticated than simple term frequency approaches. The algorithm finds minimal text spans containing all query terms and calculates density scores based on cover tightness and term weights. Multiple normalization methods are supported to handle different document characteristics. The function handles edge cases like overlapping covers and missing terms gracefully. Performance is optimized through efficient cover finding and memory management.