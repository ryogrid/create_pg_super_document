# supportSecondarySplit

## Location
[src/backend/access/gist/gistsplit.c:258-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L258-L343)

## Overview
Handles cleanup and optimization after a secondary split when the user-defined PickSplit method didn't support secondary splits, leaving split datum flags set.

## Definition

```c
union, so we just choose swap
		 * or not by lowest penalty for that side.  We can only get here if a
		 * secondary split happened to have all NULLs in its column in the
		 * tuples that the outer recursion level had assigned to one side.
		 * (Note that the null checks in gistSplitByKey don't prevent the
		 * case, because they'll only be checking tuples that were considered
		 * don't-cares at the outer recursion level, not the tuples that went
		 * into determining the passed-down left and right union keys.)
		 */
		penalty1 = gistpenalty(giststate, attno, entry1, false, &entrySL, false);
```
## Detailed Description
This function is called to clean up when a secondary split was performed but the user-defined PickSplit method didn't support it, evidenced by spl_ldatum_exists or spl_rdatum_exists flags still being true. The function performs two main tasks:

1. **Split Optimization**: Evaluates whether to swap the left and right outputs of the secondary split by calculating penalties for merging tuples into the previously chosen sets. It chooses the configuration that minimizes the total penalty.

2. **Union Key Update**: Updates the union datums for the current column by incorporating the previous union keys (oldL/oldR), since the user-defined PickSplit method didn't handle this.

The function uses penalty calculations to determine the optimal arrangement and ensures that union keys properly reflect the combined data from both the original split and the secondary split.

## Parameters / Member Variables
- `giststate`: The relation being operated on
- `attno`: GiST state information containing operator class methods
- `entry1`: The attribute number (column) being processed
- `false`: The split vector containing the results of the split operation
- `entrySL`: The previous left union key datum
- `false`: The previous right union key datum
## Dependencies
- Functions called/Symbols referenced:
  - gistentryinit
  - [gistpenalty](../g/gistpenalty.md)
  - [gistMakeUnionKey](../g/gistMakeUnionKey.md)
  - SWAPVAR (macro)
- Types referenced:
  - [GISTSTATE](../G/GISTSTATE.md)
  - [GIST_SPLITVEC](../G/GIST_SPLITVEC.md)
  - [GISTENTRY](../G/GISTENTRY.md)
- Called from:
  - [gistUserPicksplit](../g/gistUserPicksplit.md)

## Notes and Other Information
- This function is only called when secondary splits occur and the user-defined PickSplit method lacks secondary split support
- The penalty-based optimization helps maintain efficient index structure by choosing the split arrangement with lower access costs
- The function handles edge cases where only one union key exists (when one side had all NULLs)
- After processing, it resets the spl_ldatum_exists and spl_rdatum_exists flags to false

## Simplified Source

```c
static void
supportSecondarySplit(Relation r, GISTSTATE *giststate, int attno,
                      GIST_SPLITVEC *sv, Datum oldL, Datum oldR)
{
    bool leaveOnLeft = true;
    GISTENTRY entryL, entryR, entrySL, entrySR;

    // Initialize entries for old and new union keys
    gistentryinit(entryL, oldL, r, NULL, 0, false);
    gistentryinit(entryR, oldR, r, NULL, 0, false);
    gistentryinit(entrySL, sv->spl_ldatum, r, NULL, 0, false);
    gistentryinit(entrySR, sv->spl_rdatum, r, NULL, 0, false);

    if (sv->spl_ldatum_exists && sv->spl_rdatum_exists) {
        // Both unions exist - compare total penalties for both arrangements
        float penalty1 = gistpenalty(giststate, attno, &entryL, false, &entrySL, false) +
                         gistpenalty(giststate, attno, &entryR, false, &entrySR, false);
        float penalty2 = gistpenalty(giststate, attno, &entryL, false, &entrySR, false) +
                         gistpenalty(giststate, attno, &entryR, false, &entrySL, false);

        if (penalty1 > penalty2)
            leaveOnLeft = false;  // Swap gives better penalties
    } else {
        // Only one union exists - choose arrangement with lower penalty
        GISTENTRY *entry1 = (sv->spl_ldatum_exists) ? &entryL : &entryR;
        float penalty1 = gistpenalty(giststate, attno, entry1, false, &entrySL, false);
        float penalty2 = gistpenalty(giststate, attno, entry1, false, &entrySR, false);

        leaveOnLeft = (penalty1 < penalty2) ? sv->spl_ldatum_exists : sv->spl_rdatum_exists;
    }

    // Swap left and right if needed
    if (!leaveOnLeft) {
        SWAPVAR(sv->spl_left, sv->spl_right, sv->spl_left);
        SWAPVAR(sv->spl_nleft, sv->spl_nright, sv->spl_nleft);
        SWAPVAR(sv->spl_ldatum, sv->spl_rdatum, sv->spl_ldatum);
        gistentryinit(entrySL, sv->spl_ldatum, r, NULL, 0, false);
        gistentryinit(entrySR, sv->spl_rdatum, r, NULL, 0, false);
    }

    // Update union keys by merging with old keys
    if (sv->spl_ldatum_exists) {
        bool tmpBool;
        gistMakeUnionKey(giststate, attno, &entryL, false, &entrySL, false,
                         &sv->spl_ldatum, &tmpBool);
    }

    if (sv->spl_rdatum_exists) {
        bool tmpBool;
        gistMakeUnionKey(giststate, attno, &entryR, false, &entrySR, false,
                         &sv->spl_rdatum, &tmpBool);
    }

    sv->spl_ldatum_exists = sv->spl_rdatum_exists = false;
}
```