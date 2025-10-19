# _bt_preprocess_array_keys_final

## Location
[src/backend/access/nbtree/nbtutils.c:551-711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L551-L711)

## Overview
Finalizes array scan key preprocessing by fixing up scan key references, setting up ORDER procedures, and converting single-element arrays into equivalent non-array equality scan keys.

## Definition

```c
structure is protected
	 * using a spinlock, so defensively limit its size.  In practice this can
	 * only affect parallel scans that use an incomplete opfamily.
	 */
	if (scan->parallel_scan && so->numArrayKeys > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg_internal("number of array scan keys left by preprocessing (%d) exceeds the maximum allowed by parallel btree index scans (%d)",
								 so->numArrayKeys, INDEX_MAX_KEYS)));
```
## Detailed Description
This function performs the final phase of array scan key preprocessing after the main preprocessing steps are complete. It handles several critical finalization tasks:

1. **Reference Remapping**: Translates scan key references in BTArrayKeyInfo from input scan key offsets (scan->keyData[]) to output scan key offsets (so->keyData[]) using the provided keyDataMap.

2. **ORDER Procedure Setup**: 
   - Repositions existing ORDER procedures for array keys to match their new positions in so->keyData[]
   - Sets up ORDER procedures for non-array equality scan keys that survived preprocessing
   - Skips ORDER procedure setup for IS NULL scan keys and non-required scan keys

3. **Single-Element Array Optimization**: Converts array scan keys with exactly one element into equivalent non-array equality scan keys, which provides a runtime performance benefit since non-array equality operations are slightly faster than array operations.

4. **Parallel Scan Validation**: For parallel index scans, validates that the number of remaining array keys doesn't exceed INDEX_MAX_KEYS to prevent issues with shared memory structures protected by spinlocks.

The function operates in-place and can completely eliminate arrays from a scan if all arrays are reduced to single elements.

## Parameters / Member Variables
- : The index scan descriptor containing the scan keys and array information
- : Array mapping input scan key indices to output scan key indices

## Dependencies
- Functions called/Symbols referenced:
  - BTScanOpaque
  - [BTArrayKeyInfo](../B/BTArrayKeyInfo.md)
  - [_bt_setup_array_cmp](_bt_setup_array_cmp.md)
  - memmove
  - ereport
  - SK_SEARCHARRAY, SK_SEARCHNULL, SK_BT_REQFWD
  - InvalidStrategy
  - INDEX_MAX_KEYS
  - PG_USED_FOR_ASSERTS_ONLY

- Called from (representative examples):
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (final step in scan key preprocessing)

## Notes and Other Information
- Returns early if so->numArrayKeys is 0, indicating no array keys need finalization
- The function assumes that equality strategy scan keys appear in original input order within each group of entries for the same index attribute
- Single-element array transformation decrements so->numArrayKeys and may leave the scan with no arrays at all
- When arrays are removed due to single-element optimization, remaining arrays are shifted forward in the BTArrayKeyInfo array
- For parallel scans, the function enforces a limit on the number of array keys to prevent excessive shared memory usage
- The optimization of converting single-element arrays to non-array keys is purely for performance and not required for correctness

## Simplified Source

```c
static void _bt_preprocess_array_keys_final(IndexScanDesc scan, int *keyDataMap)
{
    BTScanOpaque so = (BTScanOpaque) scan->opaque;
    Relation rel = scan->indexRelation;
    int arrayidx = 0;

    // Early exit if no array keys to process
    if (so->numArrayKeys == 0)
        return;

    // Process each output equality scan key
    for (int output_ikey = 0; output_ikey < so->numberOfKeys; output_ikey++)
    {
        ScanKey outkey = so->keyData + output_ikey;
        int input_ikey;

        // Skip non-equality strategies
        if (outkey->sk_strategy != BTEqualStrategyNumber)
            continue;

        input_ikey = keyDataMap[output_ikey];

        // Handle non-array keys: set up ORDER procedures
        if (!(outkey->sk_flags & SK_SEARCHARRAY))
        {
            // Skip NULL searches and non-required keys
            if ((outkey->sk_flags & SK_SEARCHNULL) ||
                !(outkey->sk_flags & SK_BT_REQFWD))
                continue;

            // Set up ORDER procedure for comparison
            Oid elemtype = outkey->sk_subtype;
            if (elemtype == InvalidOid)
                elemtype = rel->rd_opcintype[outkey->sk_attno - 1];

            _bt_setup_array_cmp(scan, outkey, elemtype,
                               &so->orderProcs[output_ikey], NULL);
            continue;
        }

        // Handle array keys: reposition ORDER procedures
        so->orderProcs[output_ikey] = so->orderProcs[input_ikey];

        // Update array scan key references
        for (; arrayidx < so->numArrayKeys; arrayidx++)
        {
            BTArrayKeyInfo *array = &so->arrayKeys[arrayidx];

            if (array->scan_key == input_ikey)
            {
                array->scan_key = output_ikey;

                // Optimize single-element arrays to non-array keys
                if (array->num_elems == 1)
                {
                    outkey->sk_flags &= ~SK_SEARCHARRAY;
                    outkey->sk_argument = array->elem_values[0];
                    so->numArrayKeys--;

                    if (so->numArrayKeys == 0)
                        return;

                    // Shift remaining arrays forward
                    memmove(array, array + 1,
                           sizeof(BTArrayKeyInfo) * (so->numArrayKeys - arrayidx));
                }
                else
                {
                    arrayidx++;
                }
                break;
            }
        }
    }

    // Check parallel scan limits
    if (scan->parallel_scan && so->numArrayKeys > INDEX_MAX_KEYS)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg_internal("too many array scan keys for parallel btree scan")));
}
```