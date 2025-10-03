# gistUserPicksplit

## Location
[src/backend/access/gist/gistsplit.c:415-584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L415-L584)

## Overview
Invokes the user-defined picksplit method for a specific index column and handles the result, including optimization through don't-care tuple analysis and secondary split support.

## Definition

```c
union datums computed by PickSplit back to v arrays */
	v->spl_lattr[attno] = sv->spl_ldatum;
```
## Detailed Description
This function orchestrates the splitting process for a GiST index column by calling the user-defined PickSplit method and processing the results. It handles several complex scenarios:

1. **User-Defined Split Execution**: Calls the opclass-specific PickSplit method with proper collation support and prepared split vector data for secondary splits.

2. **Fallback Handling**: If the user method fails (puts all tuples on one side), it logs a debug message and falls back to genericPickSplit for a basic even distribution.

3. **Secondary Split Support**: Handles cases where previous split levels provide existing union keys by calling supportSecondarySplit when needed.

4. **Don't-Care Analysis**: Identifies tuples that could be placed on either side without penalty, enabling optimization through recursive splitting on subsequent columns.

5. **Degenerate Split Detection**: Recognizes when splits are ineffective (equal union keys) and signals the need to try the next column.

The function returns false if splitting is complete, or true if don't-care tuples exist that could benefit from analysis of additional columns.

## Parameters / Member Variables
- : The relation (index) being split
- : Vector containing the index entries to be split
- : Current attribute/column number being processed  
- : Split vector structure containing split state and results
- : Array of index tuples corresponding to the entries
- : Length of the index tuple array
- : GiST state with operator class methods and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (invokes user picksplit method)
  - [genericPickSplit](genericPickSplit.md) (fallback split method)
  - [supportSecondarySplit](../s/supportSecondarySplit.md) (handles secondary split cleanup)
  - [gistKeyIsEQ](gistKeyIsEQ.md) (compares union keys for equality)
  - [findDontCares](../f/findDontCares.md) (identifies relocatable tuples)
  - [removeDontCares](../r/removeDontCares.md) (removes don't-cares from split arrays)
  - [gistunionsubkey](gistunionsubkey.md) (recomputes union keys)
  - [placeOne](../p/placeOne.md) (assigns single don't-care tuple)
  - ereport, errcode, errmsg, errhint (error reporting)
- Types referenced:
  - [GistEntryVector](../G/GistEntryVector.md), GistSplitVector, GIST_SPLITVEC
  - [IndexTuple](../I/IndexTuple.md), GISTSTATE
  - OffsetNumber
- Constants used:
  - InvalidOffsetNumber, FirstOffsetNumber, DEBUG1
- Called from:
  - [gistSplitByKey](gistSplitByKey.md)

## Notes and Other Information
- Handles backward compatibility with old PickSplit API by fixing InvalidOffsetNumber values
- Implements sophisticated don't-care tuple optimization that can significantly improve index quality
- The single don't-care case is handled specially using penalty-based placement rather than recursion
- Secondary splits allow the method to work with union keys from parent split levels
- Error reporting provides helpful hints for users when their PickSplit methods fail
- The function's return value controls whether recursive splitting continues with additional columns

## Simplified Source

```c
static bool
gistUserPicksplit(Relation r, GistEntryVector *entryvec, int attno,
                  GistSplitVector *v, IndexTuple *itup, int len,
                  GISTSTATE *giststate)
{
    GIST_SPLITVEC *sv = &v->splitVector;

    // Prepare split vector for secondary split support
    sv->spl_ldatum_exists = !(v->spl_lisnull[attno]);
    sv->spl_rdatum_exists = !(v->spl_risnull[attno]);
    sv->spl_ldatum = v->spl_lattr[attno];
    sv->spl_rdatum = v->spl_rattr[attno];

    // Call user-defined PickSplit method
    FunctionCall2Coll(&giststate->picksplitFn[attno],
                     giststate->supportCollation[attno],
                     PointerGetDatum(entryvec),
                     PointerGetDatum(sv));

    // Handle failed split (all tuples on one side)
    if (sv->spl_nleft == 0 || sv->spl_nright == 0) {
        ereport(DEBUG1,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("picksplit method for column %d of index \"%s\" failed",
                        attno + 1, RelationGetRelationName(r)),
                 errhint("The index is not optimal. To optimize it, contact a developer, or try to use the column as the second one in the CREATE INDEX command.")));

        // Reinitialize and use generic split as fallback
        sv->spl_ldatum_exists = !(v->spl_lisnull[attno]);
        sv->spl_rdatum_exists = !(v->spl_risnull[attno]);
        sv->spl_ldatum = v->spl_lattr[attno];
        sv->spl_rdatum = v->spl_rattr[attno];

        genericPickSplit(giststate, entryvec, sv, attno);
    } else {
        // Handle compatibility with old PickSplit API
        if (sv->spl_left[sv->spl_nleft - 1] == InvalidOffsetNumber)
            sv->spl_left[sv->spl_nleft - 1] = (OffsetNumber)(entryvec->n - 1);
        if (sv->spl_right[sv->spl_nright - 1] == InvalidOffsetNumber)
            sv->spl_right[sv->spl_nright - 1] = (OffsetNumber)(entryvec->n - 1);
    }

    // Clean up secondary split if needed
    if (sv->spl_ldatum_exists || sv->spl_rdatum_exists)
        supportSecondarySplit(r, giststate, attno, sv,
                            v->spl_lattr[attno], v->spl_rattr[attno]);

    // Copy union datums back to split vector
    v->spl_lattr[attno] = sv->spl_ldatum;
    v->spl_rattr[attno] = sv->spl_rdatum;
    v->spl_lisnull[attno] = false;
    v->spl_risnull[attno] = false;

    v->spl_dontcare = NULL;

    // Check if we can optimize using additional columns
    if (attno + 1 < giststate->nonLeafTupdesc->natts) {
        // Check for degenerate split (equal union keys)
        if (gistKeyIsEQ(giststate, attno, sv->spl_ldatum, sv->spl_rdatum))
            return true;

        // Look for don't-care tuples that could be relocated
        v->spl_dontcare = (bool *) palloc0(sizeof(bool) * (entryvec->n + 1));
        int NumDontCare = findDontCares(r, giststate, entryvec->vector, v, attno);

        if (NumDontCare > 0) {
            // Remove don't-cares from split arrays
            removeDontCares(sv->spl_left, &sv->spl_nleft, v->spl_dontcare);
            removeDontCares(sv->spl_right, &sv->spl_nright, v->spl_dontcare);

            // Check if split became degenerate after removing don't-cares
            if (sv->spl_nleft == 0 || sv->spl_nright == 0) {
                v->spl_dontcare = NULL;
                return true;
            }

            // Recompute union keys excluding don't-care tuples
            gistunionsubkey(giststate, itup, v);

            if (NumDontCare == 1) {
                // Special case: place single don't-care using penalty comparison
                OffsetNumber toMove;
                for (toMove = FirstOffsetNumber; toMove < entryvec->n; toMove++) {
                    if (v->spl_dontcare[toMove])
                        break;
                }
                Assert(toMove < entryvec->n);

                placeOne(r, giststate, v, itup[toMove - 1], toMove, attno + 1);
            } else {
                // Multiple don't-cares: continue recursive splitting
                return true;
            }
        }
    }

    return false; // Split is complete
}
```