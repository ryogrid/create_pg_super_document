# gistfinishsplit

## Location
[src/backend/access/gist/gist.c:1349-1444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1349-L1444)

## Overview
Completes an incomplete GiST page split by recursively inserting and updating downlinks in parent pages for all child pages involved in the split operation.

## Definition

```c
static void
gistfinishsplit(GISTInsertState *state, GISTInsertStack *stack,
				GISTSTATE *giststate, List *splitinfo, bool unlockbuf)
```
## Detailed Description
gistfinishsplit handles the critical post-split phase of GiST page splitting by managing the recursive propagation of downlink updates up the tree. When a page is split into multiple child pages, their parent must be updated to contain appropriate downlinks for each new page.

The function processes split information from right to left, inserting downlinks for new pages one at a time until only two pages remain. It then performs a final atomic operation to insert the downlink for the last new page while updating the downlink for the original (leftmost) page. This approach ensures consistency during the complex multi-page update process.

The function implements sophisticated parent page management, including handling cases where parent pages themselves split during downlink insertion. It uses gistFindCorrectParent to locate the appropriate parent and manages lock coordination to maintain tree consistency. Upon completion, it sets retry_from_parent to handle potential path changes caused by concurrent splits.

## Parameters / Member Variables
- : GISTInsertState containing insertion context, relation information, and build state flags
- : GISTInsertStack representing the path from root to the split page, including parent page information
- : GISTSTATE with cached access method procedures and support function details
- : List of GISTPageSplitInfo structures containing child pages from left-to-right split order
- : Boolean flag indicating whether to release lock on stack->buffer upon completion

## Dependencies
- Functions called/Symbols referenced:
  - [LockBuffer](../L/LockBuffer.md) (exclusive locking for parent page modifications)
  - [list_length](../l/list_length.md), list_nth, lsecond, linitial (list manipulation for splitinfo processing)
  - [gistFindCorrectParent](gistFindCorrectParent.md) (parent page location and validation)
  - [gistinserttuples](gistinserttuples.md) (recursive downlink insertion and updates)
  - [GISTPageSplitInfo](../G/GISTPageSplitInfo.md) (split page information structure)
- Called from (representative examples):
  - [gistfixsplit](gistfixsplit.md) (split completion during recovery)
  - [gistinserttuples](gistinserttuples.md) (split handling during normal insertions)

## Notes and Other Information
- Requires caller to hold locks on stack->buffer and all child pages in splitinfo
- Always unlocks and unpins child pages upon completion regardless of unlockbuf setting
- Processes splits from right-to-left to minimize intermediate inconsistent states
- Handles parent page splits by invalidating downlinkoffnum and using gistFindCorrectParent
- Sets retry_from_parent flag to handle potential path invalidation during index builds
- Critical for maintaining GiST tree consistency during complex multi-page split operations
- The final atomic update of both original and new page downlinks prevents intermediate inconsistencies
- Essential component of the GiST split recovery and completion mechanism

## Simplified Source

```c
static void
gistfinishsplit(GISTInsertState *state, GISTInsertStack *stack,
                GISTSTATE *giststate, List *splitinfo, bool unlockbuf)
{
    GISTPageSplitInfo *right;
    GISTPageSplitInfo *left;
    IndexTuple tuples[2];

    // Lock parent page for exclusive access
    LockBuffer(stack->parent->buffer, GIST_EXCLUSIVE);

    // Insert downlinks from right to left until only two pages remain
    for (int pos = list_length(splitinfo) - 1; pos > 1; pos--)
    {
        right = (GISTPageSplitInfo *) list_nth(splitinfo, pos);
        left = (GISTPageSplitInfo *) list_nth(splitinfo, pos - 1);

        gistFindCorrectParent(state->r, stack, state->is_build);
        if (gistinserttuples(state, stack->parent, giststate,
                            &right->downlink, 1, InvalidOffsetNumber,
                            left->buf, right->buf, false, false))
        {
            // Parent split invalidates downlink position
            stack->downlinkoffnum = InvalidOffsetNumber;
        }
    }

    // Get the final two pages for atomic update
    right = (GISTPageSplitInfo *) lsecond(splitinfo);
    left = (GISTPageSplitInfo *) linitial(splitinfo);

    // Atomically insert new downlink and update original downlink
    tuples[0] = left->downlink;
    tuples[1] = right->downlink;
    gistFindCorrectParent(state->r, stack, state->is_build);
    gistinserttuples(state, stack->parent, giststate, tuples, 2,
                    stack->downlinkoffnum, left->buf, right->buf,
                    true, unlockbuf);

    // Downlink may have moved due to update implementation
    stack->downlinkoffnum = InvalidOffsetNumber;

    // Signal that path may need re-validation
    stack->retry_from_parent = true;
}
```