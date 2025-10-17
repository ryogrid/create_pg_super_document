# checkcondition_str

## Location
[src/backend/utils/adt/tsvector_op.c:1295-1462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L1295-L1462)

## Overview
Callback function for TS_execute that searches a tsvector for lexemes matching a query operand, handling both exact matches and prefix searches with optional position data collection for phrase matching.

## Definition

```c
struct, or NULL
 * Ldata, Rdata: input position lists
 * emit: bitmask of TSPO_XXX flags
 * Loffset: offset to be added to Ldata positions before comparing/outputting
 * Roffset: offset to be added to Rdata positions before comparing/outputting
 * max_npos: maximum possible required size of output position array
 *
 * Loffset and Roffset should not be negative, else we risk trying to output
 * negative positions, which won't fit into WordEntryPos.
 *
 * The result is boolean (TS_YES or TS_NO), but for the caller's convenience
 * we return it as TSTernaryValue.
 *
 * Returns TS_YES if any positions were emitted to *data;
```
## Detailed Description
The  function is the core matching engine used by PostgreSQL's text search execution framework. It implements a binary search algorithm to locate lexemes in a tsvector that match a query operand, supporting both exact string matching and prefix matching modes.

For exact matches, the function performs a standard binary search through the sorted WordEntry array. For prefix searches, it extends the search to find all lexemes where the query term is a prefix, collecting and merging position information from multiple matching entries.

When position data is requested (for phrase queries), the function aggregates positions from all matching lexemes, sorts them, removes duplicates, and returns them in the ExecPhraseData structure. The function carefully manages memory allocation and handles the complex logic of combining results from multiple prefix matches.

## Parameters / Member Variables
- **checkval**: Void pointer to CHKVAL structure containing tsvector access data
- **val**: QueryOperand structure specifying the search criteria
- **data**: Optional ExecPhraseData structure for position information collection

## Dependencies
- Functions called/Symbols referenced:
  - [tsCompareString](../t/tsCompareString.md): Core string comparison function supporting prefix matching
  - [checkclass_str](checkclass_str.md): Weight checking and position filtering function
  - [palloc](../p/palloc.md): Allocate memory in PostgreSQL memory context
  - [pfree](../p/pfree.md): Free allocated memory
  - [repalloc](../r/repalloc.md): Reallocate memory with new size
  - qsort: Standard C library sorting function
  - [qunique](../q/qunique.md): PostgreSQL utility to remove duplicates from sorted array
  - [compareWordEntryPos](compareWordEntryPos.md): Comparison function for WordEntryPos sorting
  - TSTernaryValue constants (TS_YES, TS_NO, TS_MAYBE)
  - [WordEntry](../W/WordEntry.md): Lexeme entry structure in tsvector
  - WordEntryPos: Position and weight information structure
  - [ExecPhraseData](../E/ExecPhraseData.md): Structure for collecting phrase matching positions

- Called from (representative examples):
  - [ts_match_vq](../t/ts_match_vq.md): Main tsvector-tsquery matching function

## Notes and Other Information
- Implements the TSExecuteCallback interface for the TS_execute framework
- Uses binary search for O(log n) lexeme lookup performance in sorted tsvectors
- Prefix search extends beyond exact match to find all matching prefixes
- Position aggregation uses dynamic memory allocation, starting with 256 positions and doubling as needed
- Returns TS_YES for definite match, TS_NO for no match, TS_MAYBE for uncertain cases
- Handles complex logic for combining position data from multiple prefix matches
- Memory management includes careful cleanup of temporary allocations during prefix searches  
- Position arrays are sorted and deduplicated to ensure consistent results
- Critical component in phrase query execution and proximity-based text search operations
- Supports weight-restricted searches through delegation to checkclass_str
- Optimized for the common case of exact matching while supporting the more complex prefix search scenario

## Simplified Source

```c
static TSTernaryValue checkcondition_str(void *checkval, QueryOperand *val, ExecPhraseData *data) {
    CHKVAL *chkval = (CHKVAL *) checkval;
    WordEntry *StopLow = chkval->arrb;
    WordEntry *StopHigh = chkval->arre;
    WordEntry *StopMiddle;
    TSTernaryValue res = TS_NO;

    // Binary search for exact match
    while (StopLow < StopHigh) {
        StopMiddle = StopLow + (StopHigh - StopLow) / 2;

        int difference = tsCompareString(chkval->operand + val->distance, val->length,
                                        chkval->values + StopMiddle->pos, StopMiddle->len,
                                        false);

        if (difference == 0) {
            // Found exact match - check weight and fill position data
            res = checkclass_str(chkval, StopMiddle, val, data);
            break;
        }
        else if (difference > 0)
            StopLow = StopMiddle + 1;
        else
            StopHigh = StopMiddle;
    }

    // Handle prefix search if enabled
    if (val->prefix && (res != TS_YES || data)) {
        WordEntryPos *allpos = NULL;
        int npos = 0, totalpos = 0;

        // Adjust position for corner cases
        if (StopLow >= StopHigh)
            StopMiddle = StopHigh;

        // Clear any previous data from exact match
        if (data) {
            if (data->allocated) pfree(data->pos);
            data->pos = NULL;
            data->allocated = false;
            data->npos = 0;
        }
        res = TS_NO;

        // Search for all prefix matches
        while ((res != TS_YES || data) &&
               StopMiddle < chkval->arre &&
               tsCompareString(chkval->operand + val->distance, val->length,
                              chkval->values + StopMiddle->pos, StopMiddle->len,
                              true) == 0) {

            TSTernaryValue subres = checkclass_str(chkval, StopMiddle, val, data);

            if (subres != TS_NO) {
                if (data) {
                    if (subres == TS_MAYBE) {
                        res = TS_MAYBE;
                        npos = 0;
                        if (allpos) pfree(allpos);
                        break;
                    }

                    // Expand position array if needed
                    while (npos + data->npos > totalpos) {
                        if (totalpos == 0) {
                            totalpos = 256;
                            allpos = palloc(sizeof(WordEntryPos) * totalpos);
                        } else {
                            totalpos *= 2;
                            allpos = repalloc(allpos, sizeof(WordEntryPos) * totalpos);
                        }
                    }

                    // Copy positions from this match
                    memcpy(allpos + npos, data->pos, sizeof(WordEntryPos) * data->npos);
                    npos += data->npos;

                    // Cleanup individual match data
                    if (data->allocated) pfree(data->pos);
                    data->pos = NULL;
                    data->allocated = false;
                    data->npos = 0;
                } else {
                    // No position data needed, just track YES/MAYBE
                    if (subres == TS_YES || res == TS_NO)
                        res = subres;
                }
            }
            StopMiddle++;
        }

        // Finalize position data if we collected any
        if (data && npos > 0) {
            data->pos = allpos;
            qsort(data->pos, npos, sizeof(WordEntryPos), compareWordEntryPos);
            data->npos = qunique(data->pos, npos, sizeof(WordEntryPos), compareWordEntryPos);
            data->allocated = true;
            res = TS_YES;
        }
    }

    return res;
}
```