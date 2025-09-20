# compareDocR

## Location
[src/backend/utils/adt/tsrank.c:519-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L519-L539)

## Overview
A static comparison function used for sorting DocRepresentation structures in ascending order by position, weight, and entry for text search ranking calculations.

## Definition

```c
typedef struct
{
	bool		operandexists;
	bool		reverseinsert;	/* indicates insert order, true means
								 * descending order */
	uint32		npos;
	WordEntryPos pos[MAXQROPOS];
} QueryRepresentationOperand;
```
## Detailed Description
The  function is a qsort-compatible comparison function that establishes a total ordering for DocRepresentation structures used in PostgreSQL's text search ranking system. It implements a three-level sorting hierarchy: first by word position within the document, then by weight class when positions are equal, and finally by word entry when both position and weight are identical. This ordering is essential for efficiently processing document representations during cover distance calculations and other ranking algorithms.

## Parameters

- `va` (const void*): Pointer to the first DocRepresentation structure to compare
- `vb` (const void*): Pointer to the second DocRepresentation structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - WEP_GETPOS: Macro that extracts position information from WordEntryPos (bits 0-13)
  - WEP_GETWEIGHT: Macro that extracts weight class from WordEntryPos (bits 14+)
  - DocRepresentation: Structure containing position data and word entry mapping information
- Called from (representative examples):
  - [get_docrep](../g/get_docrep.md): Uses qsort with compareDocR to sort document representation arrays

## Notes and Other Information
- Returns standard comparison values: negative for a < b, zero for a == b, positive for a > b
- Sorting priority: position (primary) → weight (secondary) → entry pointer (tertiary)
- The position comparison uses WEP_GETPOS to extract the actual position from the packed WordEntryPos format
- Weight comparison uses WEP_GETWEIGHT to extract weight class information (D=0, C=1, B=2, A=3)
- Entry pointer comparison provides deterministic ordering when position and weight are identical
- Essential for proper functioning of cover distance algorithms that rely on sorted document representations
- Part of PostgreSQL's full-text search ranking infrastructure in the tsrank.c module