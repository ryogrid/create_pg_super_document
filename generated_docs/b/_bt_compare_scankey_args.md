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

## Simplified Source

```c
static bool
_bt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
                         ScanKey leftarg, ScanKey rightarg,
                         BTArrayKeyInfo *array, FmgrInfo *orderproc,
                         bool *result)
{
    Relation rel = scan->indexRelation;
    Oid lefttype, righttype, optype, opcintype, cmp_op;
    StrategyNumber strat;

    // Handle NULL values using NULLS FIRST/LAST semantics
    if ((leftarg->sk_flags | rightarg->sk_flags) & SK_ISNULL)
    {
        bool leftnull = (leftarg->sk_flags & SK_ISNULL);
        bool rightnull = (rightarg->sk_flags & SK_ISNULL);

        strat = op->sk_strategy;
        if (op->sk_flags & SK_BT_NULLS_FIRST)
            strat = BTCommuteStrategyNumber(strat);

        // Perform NULL comparison based on strategy
        switch (strat)
        {
            case BTLessStrategyNumber:
                *result = (leftnull < rightnull);
                break;
            case BTEqualStrategyNumber:
                *result = (leftnull == rightnull);
                break;
            case BTGreaterStrategyNumber:
                *result = (leftnull > rightnull);
                break;
            // Other cases...
        }
        return true;
    }

    // Handle array scan keys specially
    if (array)
    {
        bool leftarray = ((leftarg->sk_flags & SK_SEARCHARRAY) &&
                         leftarg->sk_strategy == BTEqualStrategyNumber);
        bool rightarray = ((rightarg->sk_flags & SK_SEARCHARRAY) &&
                          rightarg->sk_strategy == BTEqualStrategyNumber);

        if (leftarray && rightarray)
            return false;  // Can't compare two arrays

        // Delegate to specialized array comparison function
        if (leftarray)
            return _bt_compare_array_scankey_args(scan, leftarg, rightarg,
                                                 orderproc, array, result);
        else if (rightarray)
            return _bt_compare_array_scankey_args(scan, rightarg, leftarg,
                                                 orderproc, array, result);
    }

    // Get data types for comparison
    opcintype = rel->rd_opcintype[leftarg->sk_attno - 1];

    lefttype = (leftarg->sk_subtype != InvalidOid) ? leftarg->sk_subtype : opcintype;
    righttype = (rightarg->sk_subtype != InvalidOid) ? rightarg->sk_subtype : opcintype;
    optype = (op->sk_subtype != InvalidOid) ? op->sk_subtype : opcintype;

    // Use pre-loaded function if types match
    if (lefttype == opcintype && righttype == optype)
    {
        *result = DatumGetBool(FunctionCall2Coll(&op->sk_func,
                                                op->sk_collation,
                                                leftarg->sk_argument,
                                                rightarg->sk_argument));
        return true;
    }

    // Look up cross-type operator in opfamily
    strat = op->sk_strategy;
    if (op->sk_flags & SK_BT_DESC)
        strat = BTCommuteStrategyNumber(strat);

    cmp_op = get_opfamily_member(rel->rd_opfamily[leftarg->sk_attno - 1],
                                lefttype, righttype, strat);

    if (OidIsValid(cmp_op))
    {
        RegProcedure cmp_proc = get_opcode(cmp_op);
        if (RegProcedureIsValid(cmp_proc))
        {
            *result = DatumGetBool(OidFunctionCall2Coll(cmp_proc,
                                                       op->sk_collation,
                                                       leftarg->sk_argument,
                                                       rightarg->sk_argument));
            return true;
        }
    }

    // Comparison not possible
    return false;
}
```