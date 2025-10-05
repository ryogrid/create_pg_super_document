# _bt_preprocess_keys

## Location
[src/backend/access/nbtree/nbtutils.c:2552-3005](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L2552-L3005)

## Overview
Preprocesses scan keys by transforming, sorting, eliminating redundancies, detecting contradictions, and marking keys as required for continuing the scan.

## Definition

```c
void
_bt_preprocess_keys(IndexScanDesc scan)
```
## Detailed Description
This function is a comprehensive scan key preprocessing routine that transforms the input scan keys from scan->keyData[] into processed output keys in so->keyData[]. It performs multiple critical operations:

1. **Key Transformation**: Applies index options (DESC, NULLS_FIRST) and commutes strategy numbers for DESC columns
2. **Redundancy Elimination**: Keeps only the tightest bounds (one = key, or one >/>= and one </<= key per attribute)  
3. **Contradiction Detection**: Identifies impossible conditions like "x = 1 AND x > 2" and sets qual_ok = false
4. **Required Key Marking**: Marks keys with SK_BT_REQFWD/SK_BT_REQBKWD flags based on scan continuation requirements
5. **Array Key Processing**: Handles SK_SEARCHARRAY keys through specialized array preprocessing

The function implements a sophisticated algorithm for determining which keys must be satisfied to continue scanning. Keys for leading attributes with equality conditions are marked as required in both directions. For the first non-equality attribute, < and <= keys are marked as forward-required while > and >= keys are marked as backward-required.

The preprocessing handles incomplete operator families gracefully - if cross-type operators are missing, redundant keys may not be eliminated, but the scan will still work correctly.

## Parameters / Member Variables
- `scan`: Index scan descriptor containing input keys in keyData[] and receiving processed keys in opaque->keyData[]
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_verify_keys_with_arraykeys](_bt_verify_keys_with_arraykeys.md)
  - [_bt_preprocess_array_keys](_bt_preprocess_array_keys.md)
  - [_bt_fix_scankey_strategy](_bt_fix_scankey_strategy.md)
  - [_bt_mark_scankey_required](_bt_mark_scankey_required.md)
  - [_bt_compare_scankey_args](_bt_compare_scankey_args.md)
  - [_bt_preprocess_array_keys_final](_bt_preprocess_array_keys_final.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [_bt_first](_bt_first.md)

## Notes and Other Information
- Only performs preprocessing once per btrescan - subsequent calls are no-ops
- Expects input keys to be sorted by attribute (verified with assertions)
- Handles row comparison keys by passing them through without modification
- Creates temporary keyDataMap for remapping orderProc arrays when array keys are present
- Sets so->qual_ok = false and returns early when contradictory or unmatchable conditions are detected
- For single key optimization, bypasses most processing but still applies indoption transformations
- Maintains array key ordering consistency required by _bt_advance_array_keys
- The numberOfEqualCols tracking is crucial for determining which subsequent keys can be marked as required
- Returns with so->numberOfKeys set to the number of processed output keys (may be less than input)

## Simplified Source

```c
void
_bt_preprocess_keys(IndexScanDesc scan)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    int numberOfKeys = scan->numberOfKeys;
    int16 *indoption = scan->indexRelation->rd_indoption;
    int new_numberOfKeys;
    int numberOfEqualCols;
    ScanKey inkeys, outkeys, cur;
    BTScanKeyPreproc xform[BTMaxStrategyNumber];
    bool test_result;
    int i, j;
    AttrNumber attno;
    ScanKey arrayKeyData;
    int *keyDataMap = NULL;
    int arrayidx = 0;

    // Skip if already preprocessed
    if (so->numberOfKeys > 0) {
        Assert(_bt_verify_keys_with_arraykeys(scan));
        return;
    }

    // Initialize result variables
    so->qual_ok = true;
    so->numberOfKeys = 0;

    if (numberOfKeys < 1)
        return;  // No keys to process

    // Handle array keys preprocessing
    arrayKeyData = _bt_preprocess_array_keys(scan);
    if (!so->qual_ok) {
        return;  // Unmatchable array
    }

    // Use preprocessed array data or original scan keys
    if (arrayKeyData) {
        inkeys = arrayKeyData;
        keyDataMap = MemoryContextAlloc(so->arrayContext,
                                       numberOfKeys * sizeof(int));
    } else {
        inkeys = scan->keyData;
    }

    outkeys = so->keyData;
    cur = &inkeys[0];

    // Verify input keys are ordered by attribute
    if (cur->sk_attno < 1)
        elog(ERROR, "btree index keys must be ordered by attribute");

    // Fast path for single key
    if (numberOfKeys == 1) {
        if (!_bt_fix_scankey_strategy(cur, indoption))
            so->qual_ok = false;
        memcpy(outkeys, cur, sizeof(ScanKeyData));
        so->numberOfKeys = 1;
        if (cur->sk_attno == 1)
            _bt_mark_scankey_required(outkeys);
        return;
    }

    // Full preprocessing for multiple keys
    new_numberOfKeys = 0;
    numberOfEqualCols = 0;
    attno = 1;
    memset(xform, 0, sizeof(xform));

    // Process each input key
    for (i = 0;; cur++, i++) {

        // Apply index option transformations
        if (i < numberOfKeys) {
            if (!_bt_fix_scankey_strategy(cur, indoption)) {
                so->qual_ok = false;
                return;
            }
        }

        // Process keys for completed attribute
        if (i == numberOfKeys || cur->sk_attno != attno) {
            int priorNumberOfEqualCols = numberOfEqualCols;

            // Verify key ordering
            if (i < numberOfKeys && cur->sk_attno < attno)
                elog(ERROR, "btree index keys must be ordered by attribute");

            // Handle equality key - eliminates other keys or detects contradictions
            if (xform[BTEqualStrategyNumber - 1].skey) {
                ScanKey eq = xform[BTEqualStrategyNumber - 1].skey;

                // Check for contradictions with other strategies
                for (j = BTMaxStrategyNumber; --j >= 0;) {
                    ScanKey chk = xform[j].skey;

                    if (!chk || j == (BTEqualStrategyNumber - 1))
                        continue;

                    if (eq->sk_flags & SK_SEARCHNULL) {
                        // IS NULL contradicts everything else
                        so->qual_ok = false;
                        return;
                    }

                    if (_bt_compare_scankey_args(scan, chk, eq, chk,
                                                NULL, NULL, &test_result)) {
                        if (!test_result) {
                            // Contradictory keys
                            so->qual_ok = false;
                            return;
                        }
                        // Eliminate redundant key
                        xform[j].skey = NULL;
                        xform[j].ikey = -1;
                    }
                }
                numberOfEqualCols++;
            }

            // Eliminate redundant < vs <= keys
            if (xform[BTLessStrategyNumber - 1].skey &&
                xform[BTLessEqualStrategyNumber - 1].skey) {
                ScanKey lt = xform[BTLessStrategyNumber - 1].skey;
                ScanKey le = xform[BTLessEqualStrategyNumber - 1].skey;

                if (_bt_compare_scankey_args(scan, le, lt, le, NULL, NULL,
                                            &test_result)) {
                    if (test_result)
                        xform[BTLessEqualStrategyNumber - 1].skey = NULL;
                    else
                        xform[BTLessStrategyNumber - 1].skey = NULL;
                }
            }

            // Eliminate redundant > vs >= keys
            if (xform[BTGreaterStrategyNumber - 1].skey &&
                xform[BTGreaterEqualStrategyNumber - 1].skey) {
                ScanKey gt = xform[BTGreaterStrategyNumber - 1].skey;
                ScanKey ge = xform[BTGreaterEqualStrategyNumber - 1].skey;

                if (_bt_compare_scankey_args(scan, ge, gt, ge, NULL, NULL,
                                            &test_result)) {
                    if (test_result)
                        xform[BTGreaterEqualStrategyNumber - 1].skey = NULL;
                    else
                        xform[BTGreaterStrategyNumber - 1].skey = NULL;
                }
            }

            // Output cleaned keys and mark as required if appropriate
            for (j = BTMaxStrategyNumber; --j >= 0;) {
                if (xform[j].skey) {
                    ScanKey outkey = &outkeys[new_numberOfKeys++];

                    memcpy(outkey, xform[j].skey, sizeof(ScanKeyData));
                    if (arrayKeyData)
                        keyDataMap[new_numberOfKeys - 1] = xform[j].ikey;
                    if (priorNumberOfEqualCols == attno - 1)
                        _bt_mark_scankey_required(outkey);
                }
            }

            if (i == numberOfKeys)
                break;

            // Reset for next attribute
            attno = cur->sk_attno;
            memset(xform, 0, sizeof(xform));
        }

        j = cur->sk_strategy - 1;

        // Handle row comparison keys directly
        if (cur->sk_flags & SK_ROW_HEADER) {
            ScanKey outkey = &outkeys[new_numberOfKeys++];

            memcpy(outkey, cur, sizeof(ScanKeyData));
            if (arrayKeyData)
                keyDataMap[new_numberOfKeys - 1] = i;
            if (numberOfEqualCols == attno - 1)
                _bt_mark_scankey_required(outkey);
            continue;
        }

        // Skip redundant array keys
        if (cur->sk_strategy == InvalidStrategy) {
            Assert(arrayKeyData && (cur->sk_flags & SK_SEARCHARRAY));
            continue;
        }

        // Track array keys
        if (cur->sk_strategy == BTEqualStrategyNumber &&
            (cur->sk_flags & SK_SEARCHARRAY)) {
            Assert(arrayKeyData);
            arrayidx++;
        }

        // Store or compare with existing key of same strategy
        if (xform[j].skey == NULL) {
            // First key of this strategy for this attribute
            xform[j].skey = cur;
            xform[j].ikey = i;
            xform[j].arrayidx = arrayidx;
        } else {
            // Compare with existing key to find more restrictive one
            if (_bt_compare_scankey_args(scan, cur, cur, xform[j].skey,
                                        NULL, NULL, &test_result)) {
                if (test_result) {
                    // New key is more restrictive
                    xform[j].skey = cur;
                    xform[j].ikey = i;
                    xform[j].arrayidx = arrayidx;
                } else if (j == (BTEqualStrategyNumber - 1)) {
                    // Contradictory equality keys
                    so->qual_ok = false;
                    return;
                }
            } else {
                // Can't compare - keep both keys
                ScanKey outkey = &outkeys[new_numberOfKeys++];

                memcpy(outkey, xform[j].skey, sizeof(ScanKeyData));
                if (arrayKeyData)
                    keyDataMap[new_numberOfKeys - 1] = xform[j].ikey;
                if (numberOfEqualCols == attno - 1)
                    _bt_mark_scankey_required(outkey);

                xform[j].skey = cur;
                xform[j].ikey = i;
                xform[j].arrayidx = arrayidx;
            }
        }
    }

    so->numberOfKeys = new_numberOfKeys;

    // Fix array references and consolidate orderProc array
    if (arrayKeyData)
        _bt_preprocess_array_keys_final(scan, keyDataMap);
}
```