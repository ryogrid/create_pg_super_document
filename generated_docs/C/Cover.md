# Cover

## Location
[src/backend/utils/adt/tsrank.c:646-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L646-L726)

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
  - [QueryRepresentation](../Q/QueryRepresentation.md) (struct type)
  - CoverExt (struct type)
  - DocRepresentation (struct type)
  - [check_stack_depth](../c/check_stack_depth.md) (recursion depth check)
  - [resetQueryRepresentation](../r/resetQueryRepresentation.md) (reset query state)
  - [fillQueryRepresentationData](../f/fillQueryRepresentationData.md) (populate operand data)
  - [TS_execute](../T/TS_execute.md) (execute query condition)
  - GETQUERY (macro to get query)
  - [checkcondition_QueryOperand](../c/checkcondition_QueryOperand.md) (condition checker function)
  - TS_EXEC_EMPTY (execution flag)
  - WEP_GETPOS (extract word position)
  - [Cover](Cover.md) (recursive self-call)
- Called from (representative examples):
  - [Cover](Cover.md) (recursive call at line 723)
  - [calc_rank_cd](../c/calc_rank_cd.md) (called at line 887)

## Notes and Other Information
This is a recursive function that includes stack depth checking to prevent overflow. The algorithm is optimized for tail-recursion and implements a sliding window approach to find minimal covers. The function returns true if a valid cover is found and false otherwise. The two-phase scanning (forward then backward) ensures optimal cover detection while the recursive nature allows exploration of multiple potential covers to find the shortest one.

## Simplified Source

```c
static bool Cover(DocRepresentation *doc, int len, QueryRepresentation *qr, CoverExt *ext) {
    DocRepresentation *ptr;
    int lastpos = ext->pos;
    bool found = false;

    // Prevent stack overflow during recursion
    check_stack_depth();

    resetQueryRepresentation(qr, false);
    ext->p = INT_MAX;
    ext->q = 0;
    ptr = doc + ext->pos;

    // Phase 1: Find upper bound of cover by scanning forward
    while (ptr - doc < len) {
        fillQueryRepresentationData(qr, ptr);

        if (TS_execute(GETQUERY(qr->query), (void *) qr,
                      TS_EXEC_EMPTY, checkcondition_QueryOperand)) {
            if (WEP_GETPOS(ptr->pos) > ext->q) {
                ext->q = WEP_GETPOS(ptr->pos);
                ext->end = ptr;
                lastpos = ptr - doc;
                found = true;
            }
            break;
        }
        ptr++;
    }

    if (!found)
        return false;

    resetQueryRepresentation(qr, true);
    ptr = doc + lastpos;

    // Phase 2: Find lower bound of cover by scanning backward
    while (ptr >= doc + ext->pos) {
        fillQueryRepresentationData(qr, ptr);

        if (TS_execute(GETQUERY(qr->query), (void *) qr,
                      TS_EXEC_EMPTY, checkcondition_QueryOperand)) {
            if (WEP_GETPOS(ptr->pos) < ext->p) {
                ext->begin = ptr;
                ext->p = WEP_GETPOS(ptr->pos);
            }
            break;
        }
        ptr--;
    }

    // Check if valid cover found and set position for next iteration
    if (ext->p <= ext->q) {
        ext->pos = (ptr - doc) + 1;
        return true;
    }

    // Recursively try next position if current cover invalid
    ext->pos++;
    return Cover(doc, len, qr, ext);
}
```