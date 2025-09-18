# Cover

## Location
src/backend/utils/adt/tsrank.c: 646 - 726

## Overview
Finds the shortest text span (cover) that contains all query terms, implementing a recursive algorithm to identify optimal text coverage for ranking calculations.

## Definition
static bool Cover(DocRepresentation *doc, int len, QueryRepresentation *qr, CoverExt *ext)

## Detailed Description
This function implements a sophisticated algorithm to find the shortest contiguous span of text that satisfies the given query. It uses a two-phase approach: first scanning forward to find the upper bound where the query is satisfied, then scanning backward to find the lower bound. The algorithm recursively attempts to find better (shorter) covers by advancing the starting position. This is a core component of PostgreSQL's text search ranking system, helping to determine how tightly query terms are clustered in the document.

## Parameters / Member Variables
- `doc`: Array of DocRepresentation structures representing the document
- `len`: Length of the document array
- `qr`: QueryRepresentation structure containing query operand data
- `ext`: CoverExt structure for tracking cover boundaries and position state

## Dependencies
- Functions called/Symbols referenced:
  - QueryRepresentation (struct type)
  - CoverExt (struct type)
  - DocRepresentation (struct type)
  - check_stack_depth (recursion depth check)
  - resetQueryRepresentation (reset query state)
  - fillQueryRepresentationData (populate operand data)
  - TS_execute (execute query condition)
  - GETQUERY (macro to get query)
  - checkcondition_QueryOperand (condition checker function)
  - TS_EXEC_EMPTY (execution flag)
  - WEP_GETPOS (extract word position)
  - Cover (recursive self-call)
- Called from (representative examples):
  - Cover (recursive call at line 723)
  - calc_rank_cd (called at line 887)

## Notes and Other Information
This is a recursive function that includes stack depth checking to prevent overflow. The algorithm is optimized for tail-recursion and implements a sliding window approach to find minimal covers. The function returns true if a valid cover is found and false otherwise. The two-phase scanning (forward then backward) ensures optimal cover detection while the recursive nature allows exploration of multiple potential covers to find the shortest one.