# shimTriConsistentFn

## Location
[src/backend/access/gin/ginlogic.c:148-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginlogic.c#L148-L226)

## Overview
A static function that implements a tri-state consistency check for GIN (Generalized Inverted iNdex) scans by testing all possible combinations of MAYBE inputs to determine the overall result.

## Definition
```c
static GinTernaryValue shimTriConsistentFn(GinScanKey key)
```

## Detailed Description
This function serves as a shim layer that provides tri-state consistent functionality when the opclass does not supply a native tri-consistent function. It operates by systematically testing all possible TRUE/FALSE combinations for entries marked as MAYBE to determine if the consistent function returns a uniform result.

The algorithm follows this strategy:
1. Identifies all MAYBE entries in the scan key
2. If there are too many MAYBE entries (>MAX_MAYBE_ENTRIES), returns MAYBE immediately
3. Tests all 2^n combinations by replacing MAYBE values with TRUE/FALSE
4. If all combinations yield the same boolean result, returns that result
5. If results differ across combinations, returns MAYBE
6. Handles recheck conditions by treating TRUE+recheck as MAYBE

The function modifies the entryRes array during processing but restores original MAYBE values before returning, making it safe for single-threaded operation.

## Parameters / Member Variables
- `key`: A GinScanKey structure containing the scan entries and their consistency states that need to be evaluated

## Dependencies
- Functions called/Symbols referenced:
  - [directBoolConsistentFn](../d/directBoolConsistentFn.md)
  - GinTernaryValue (enum type)
  - [GinScanKey](../G/GinScanKey.md) (struct type)
  - MAX_MAYBE_ENTRIES (constant)
  - GIN_MAYBE, GIN_TRUE, GIN_FALSE (enum values)
- Called from (representative examples):
  - [ginInitConsistentFunction](../g/ginInitConsistentFunction.md)

## Notes and Other Information
- This is a static function internal to ginlogic.c and not exposed outside the module
- The function has O(2^n) complexity where n is the number of MAYBE entries, making it only feasible for small numbers of uncertain inputs
- Thread safety consideration: The function modifies the key->entryRes array temporarily, which could be problematic in multithreaded GIN scans
- The recheck mechanism is used to handle cases where the consistent function needs to re-examine tuples at a higher level
- Located at src/backend/access/gin/ginlogic.c:148-226

## Simplified Source

```c
static GinTernaryValue
shimTriConsistentFn(GinScanKey key)
{
    int nmaybe = 0;
    int maybeEntries[MAX_MAYBE_ENTRIES];
    bool recheck = false;
    GinTernaryValue curResult;

    // Count MAYBE entries and store their indexes
    for (int i = 0; i < key->nentries; i++) {
        if (key->entryRes[i] == GIN_MAYBE) {
            if (nmaybe >= MAX_MAYBE_ENTRIES)
                return GIN_MAYBE;  // Too many uncertain entries
            maybeEntries[nmaybe++] = i;
        }
    }

    // If no MAYBE entries, call consistent function directly
    if (nmaybe == 0)
        return directBoolConsistentFn(key);

    // Test first combination: all MAYBE entries set to FALSE
    for (int i = 0; i < nmaybe; i++)
        key->entryRes[maybeEntries[i]] = GIN_FALSE;

    curResult = directBoolConsistentFn(key);
    recheck = key->recheckCurItem;

    // Test all other combinations (binary counting through MAYBE positions)
    for (;;) {
        // Increment to next combination
        int i;
        for (i = 0; i < nmaybe; i++) {
            if (key->entryRes[maybeEntries[i]] == GIN_FALSE) {
                key->entryRes[maybeEntries[i]] = GIN_TRUE;
                break;
            } else {
                key->entryRes[maybeEntries[i]] = GIN_FALSE;
            }
        }
        if (i == nmaybe)
            break;  // All combinations tested

        bool newResult = directBoolConsistentFn(key);
        recheck |= key->recheckCurItem;

        // If results differ, overall result is uncertain
        if (curResult != newResult) {
            curResult = GIN_MAYBE;
            break;
        }
    }

    // TRUE with recheck requirement means uncertainty
    if (curResult == GIN_TRUE && recheck)
        curResult = GIN_MAYBE;

    // Restore original MAYBE values
    for (int i = 0; i < nmaybe; i++)
        key->entryRes[maybeEntries[i]] = GIN_MAYBE;

    return curResult;
}
```