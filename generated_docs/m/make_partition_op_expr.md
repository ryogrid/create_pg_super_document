# make_partition_op_expr

## Location
[src/backend/partitioning/partbounds.c:3868-3982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3868-L3982)

## Overview
Creates an expression node for a partition key column operation with specified left and right operands, handling different partitioning strategies.

## Definition
```c
static Expr *make_partition_op_expr(PartitionKey key, int keynum, uint16 strategy, Expr *arg1, Expr *arg2)
```

## Detailed Description
This function constructs appropriate expression nodes for partition constraint operations based on the partitioning strategy. For list partitioning, it creates either ScalarArrayOpExpr nodes (for multiple values using ANY operator) or chains of OR'd equality expressions. For range partitioning, it creates simple OpExpr nodes with comparison operators. The function handles type coercion by applying RelabelType nodes when necessary, ensuring type compatibility between operands and operators. Hash partitioning is not supported and triggers an assertion failure.

## Parameters / Member Variables
- `key`: Pointer to PartitionKey structure containing partition metadata
- `keynum`: Index of the partition key column (0-based)
- `strategy`: Strategy number for the operator (e.g., BTEqualStrategyNumber)
- `arg1`: Left operand expression (typically the partition key column reference)
- `arg2`: Right operand expression (partition bound value or list of values)

## Dependencies
- Functions called/Symbols referenced:
  - [get_partition_operator](../g/get_partition_operator.md)
  - [makeRelabelType](makeRelabelType.md)
  - type_is_array
  - [get_array_type](../g/get_array_type.md)
  - [get_opcode](../g/get_opcode.md)
  - [make_opclause](make_opclause.md)
  - [makeBoolExpr](makeBoolExpr.md)
  - list_make2
  - ArrayExpr
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md)
  - PARTITION_STRATEGY_LIST
  - PARTITION_STRATEGY_RANGE
  - PARTITION_STRATEGY_HASH
  - OR_EXPR
  - COERCE_EXPLICIT_CAST
- Called from (representative examples):
  - compare_range_bounds
  - [get_qual_for_list](../g/get_qual_for_list.md)
  - [get_qual_for_range](../g/get_qual_for_range.md)

## Notes and Other Information
- This is a static function internal to partbounds.c
- Essential for building partition constraint expressions during query planning
- Handles type coercion automatically when partition key type differs from operator class type
- For list partitioning with multiple values, creates optimized ScalarArrayOpExpr when possible
- Falls back to OR'd equality expressions for list partitioning with array types or single values
- [Range](../R/Range.md) partitioning always produces simple comparison expressions
- [Hash](../H/Hash.md) partitioning support is explicitly disabled with Assert(false)
- Applies proper collation settings from the partition key metadata
- Returns NULL only in error cases or unsupported scenarios

## Simplified Source

```c
static Expr *make_partition_op_expr(PartitionKey key, int keynum, uint16 strategy, Expr *arg1, Expr *arg2) {
    Oid operoid;
    bool need_relabel = false;
    Expr *result = NULL;

    // Get the appropriate operator for this partition column
    operoid = get_partition_operator(key, keynum, strategy, &need_relabel);

    // Apply type coercion to non-Const operand if needed
    if (!IsA(arg1, Const) && (need_relabel || key->partcollation[keynum] != key->parttypcoll[keynum])) {
        arg1 = (Expr *) makeRelabelType(arg1, key->partopcintype[keynum], -1,
                                       key->partcollation[keynum], COERCE_EXPLICIT_CAST);
    }

    // Build expression based on partitioning strategy
    switch (key->strategy) {
        case PARTITION_STRATEGY_LIST: {
            List *elems = (List *) arg2;
            int nelems = list_length(elems);

            if (nelems > 1 && !type_is_array(key->parttypid[keynum])) {
                // Multiple values: create "column = ANY(ARRAY[val1, val2, ...])"
                ArrayExpr *arrexpr = makeNode(ArrayExpr);
                arrexpr->array_typeid = get_array_type(key->parttypid[keynum]);
                arrexpr->elements = elems;
                // Set other array properties...

                ScalarArrayOpExpr *saopexpr = makeNode(ScalarArrayOpExpr);
                saopexpr->opno = operoid;
                saopexpr->useOr = true;
                saopexpr->args = list_make2(arg1, arrexpr);
                // Set other scalar array op properties...

                result = (Expr *) saopexpr;
            } else {
                // Single value or array type: create OR'd equality expressions
                List *elemops = NIL;
                foreach(lc, elems) {
                    Expr *elem = lfirst(lc);
                    Expr *elemop = make_opclause(operoid, BOOLOID, false, arg1, elem,
                                               InvalidOid, key->partcollation[keynum]);
                    elemops = lappend(elemops, elemop);
                }
                result = (nelems > 1) ? makeBoolExpr(OR_EXPR, elemops, -1) : linitial(elemops);
            }
            break;
        }

        case PARTITION_STRATEGY_RANGE:
            // Simple comparison operation
            result = make_opclause(operoid, BOOLOID, false, arg1, arg2,
                                 InvalidOid, key->partcollation[keynum]);
            break;

        case PARTITION_STRATEGY_HASH:
            // Not supported
            Assert(false);
            break;
    }

    return result;
}
```