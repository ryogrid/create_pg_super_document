# _bt_compare_scankey_args

## Location
[src/backend/access/nbtree/nbtutils.c:3122-3328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L3122-L3328)

## Overview
Compares two scankey values using a specified operator, handling cross-data-type situations and array scankeys with proper opfamily member lookup and redundancy elimination.

## Definition
static bool _bt_compare_scankey_args(IndexScanDesc scan, ScanKey op, ScanKey leftarg, ScanKey rightarg, BTArrayKeyInfo *array, FmgrInfo *orderproc, bool *result)

## Detailed Description
This function performs logical comparison "leftarg op rightarg" where leftarg and rightarg are sk_argument values from ScanKeys, and the comparison operator comes from the op ScanKey. The function handles several complex scenarios:

1. **Cross-data-type comparisons**: When operand types differ, it looks up the appropriate operator in the index's opfamily using amopstrategy, amoplefttype, and amoprighttype.

2. **NULL handling**: Processes IS NULL/NOT NULL conditions with proper NULLS FIRST/NULLS LAST semantics by flipping strategies as needed.

3. **Array scankey processing**: Applies array-specific rules to determine redundant elements and delegates to _bt_compare_array_scankey_args for specialized array handling.

4. **Operator lookup**: When types don't match the pre-loaded operator, performs syscache lookups to find appropriate opfamily members.

The function is essential for scan key preprocessing and redundancy elimination during B-tree index scans.

## Parameters / Member Variables
- : IndexScanDesc - The index scan descriptor containing relation and opfamily information
- : ScanKey - The scan key containing the comparison operator and strategy
- : ScanKey - The left operand scan key
- : ScanKey - The right operand scan key  
- : BTArrayKeyInfo * - Array key metadata (NULL if no arrays involved)
- : FmgrInfo * - Ordering procedure information (currently unused)
- : bool * - Output parameter storing the comparison result

## Dependencies
- Functions called/Symbols referenced:
  - BTCommuteStrategyNumber (strategy flipping for DESC/NULLS FIRST)
  - [_bt_compare_array_scankey_args](_bt_compare_array_scankey_args.md) (specialized array comparison)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (direct function call for same-type comparisons)
  - [get_opfamily_member](../g/get_opfamily_member.md) (cross-type operator lookup)
  - [get_opcode](../g/get_opcode.md) (operator procedure lookup)
  - [OidFunctionCall2Coll](../O/OidFunctionCall2Coll.md) (cross-type function call)
  - Various strategy constants (BTLessStrategyNumber, BTEqualStrategyNumber, etc.)
  - [Scan](../S/Scan.md) key flags (SK_ISNULL, SK_SEARCHARRAY, SK_BT_NULLS_FIRST, SK_BT_DESC)

- Called from (representative examples):
  - [_bt_preprocess_keys](_bt_preprocess_keys.md) (multiple calls during scan key preprocessing and redundancy elimination)

## Notes and Other Information
- Returns true if comparison was successful, false if comparison could not be made
- Insensitive to DESC options - "x < 4" is tighter than "x < 5" regardless of sort direction
- Handles aliasing where op points to the same ScanKey as leftarg or rightarg
- Cannot handle pairs of array scan keys (relies on _bt_preprocess_array_keys to merge them)
- May shrink caller's array by eliminating redundant elements during processing
- Uses InvalidOid convention where sk_subtype defaults to opclass input type
- Part of PostgreSQL's scan key optimization and redundancy elimination system