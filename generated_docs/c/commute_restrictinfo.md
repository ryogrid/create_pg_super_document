# commute_restrictinfo

## Location
[src/backend/optimizer/util/restrictinfo.c:359-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L359-L415)

## Overview
Creates a new RestrictInfo representing the commuted version of a binary operator clause, swapping left and right operands while preserving optimization metadata and cached information.

## Definition

```c
RestrictInfo *
commute_restrictinfo(RestrictInfo *rinfo, Oid comm_op)
```
## Detailed Description
This function produces a commuted version of a RestrictInfo containing a binary operator clause by creating new OpExpr and RestrictInfo structures with swapped operands. It performs efficient flat-copy operations of the original structures and then selectively updates only the fields that need to change for commutation. The function preserves valuable cached optimization data like selectivity estimates and cost information, while properly swapping left/right relation sets and equivalence class information. It's designed specifically for use with derived index qualifications where the commuted form may provide better optimization opportunities.

## Parameters / Member Variables
- : The source RestrictInfo containing a binary operator clause to be commuted
- : The OID of the commutator operator (must be provided by the caller after lookup)

## Dependencies
- Functions called/Symbols referenced:
  - [OpExpr](../O/OpExpr.md) (type casting and structure creation)
  - lsecond
  - list_make2
- Called from (representative examples):
  - [match_opclause_to_indexcol](../m/match_opclause_to_indexcol.md)
  - make_simple_restrictinfo

## Notes and Other Information
- Efficient implementation: Uses flat-copy (memcpy) operations to duplicate structures, then selectively updates only the fields that need modification for commutation
- Shared sub-structure warning: The result shares sub-structure with the original RestrictInfo, which is acceptable for derived index quals but could be problematic if the source is subject to change
- Preserved optimization data: Maintains cached selectivity estimates, cost information, and parent equivalence class information since these should be identical for the commuted clause
- Operator class assumption: Assumes without verification that the commutator operator belongs to the same btree and hash operator classes as the original operator
- [Hash](../H/Hash.md) join handling: Updates the hashjoinoperator field only if it matched the original operator, otherwise sets it to InvalidOid
- Statistical data swapping: Properly swaps left/right bucket sizes and most common value frequencies to maintain accurate optimization statistics
- Cache invalidation: Clears the scansel_cache as it's not worth updating, and resets hash equality operators to InvalidOid for recalculation
- Serial number preservation: Maintains the same rinfo_serial number to preserve debugging and tracking consistency

## Simplified Source

```c
RestrictInfo *
commute_restrictinfo(RestrictInfo *rinfo, Oid comm_op)
{
    RestrictInfo *result;
    OpExpr *newclause;
    OpExpr *clause = castNode(OpExpr, rinfo->clause);

    Assert(list_length(clause->args) == 2);

    // Create new OpExpr with swapped arguments
    newclause = makeNode(OpExpr);
    memcpy(newclause, clause, sizeof(OpExpr));
    newclause->opno = comm_op;
    newclause->opfuncid = InvalidOid;
    newclause->args = list_make2(lsecond(clause->args),
                                linitial(clause->args));

    // Create new RestrictInfo with swapped left/right metadata
    result = makeNode(RestrictInfo);
    memcpy(result, rinfo, sizeof(RestrictInfo));

    // Update fields that need to change for commutation
    result->clause = (Expr *) newclause;
    result->left_relids = rinfo->right_relids;
    result->right_relids = rinfo->left_relids;
    result->left_ec = rinfo->right_ec;
    result->right_ec = rinfo->left_ec;
    result->left_em = rinfo->right_em;
    result->right_em = rinfo->left_em;
    result->scansel_cache = NIL;  // Clear cache, not worth updating

    // Update hash join operator if it matches the original
    if (rinfo->hashjoinoperator == clause->opno)
        result->hashjoinoperator = comm_op;
    else
        result->hashjoinoperator = InvalidOid;

    // Swap statistical data for optimization
    result->left_bucketsize = rinfo->right_bucketsize;
    result->right_bucketsize = rinfo->left_bucketsize;
    result->left_mcvfreq = rinfo->right_mcvfreq;
    result->right_mcvfreq = rinfo->left_mcvfreq;
    result->left_hasheqoperator = InvalidOid;
    result->right_hasheqoperator = InvalidOid;

    return result;
}
```