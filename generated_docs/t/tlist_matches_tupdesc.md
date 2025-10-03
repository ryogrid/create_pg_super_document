# tlist_matches_tupdesc

## Location
[src/backend/executor/execUtils.c:585-646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L585-L646)

## Overview
Determines whether a target list exactly matches a tuple descriptor, enabling projection optimization by detecting when no tuple transformation is needed.

## Definition
```c
static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, int varno, TupleDesc tupdesc)
```

## Detailed Description
tlist_matches_tupdesc performs a detailed comparison between a target list and a tuple descriptor to determine if they represent identical tuple structures. This function is crucial for projection optimization, as it identifies cases where input tuples can be passed through directly without any transformation.

The function validates multiple criteria for each attribute:
1. Target list and tuple descriptor must have the same number of attributes
2. Each target list entry must be a simple Var node (not a complex expression)
3. Var nodes must reference the correct variable number and attribute number
4. Attribute types must match (with special handling for typmod variations)
5. No dropped columns or missing values are allowed in the tuple descriptor

The function handles a special case for typmod mismatches: when a Var has typmod -1 but the tuple descriptor has a specific typmod, this is considered a match since the Var still describes the same column, just less specifically.

## Parameters / Member Variables
- `ps`: PlanState structure (used for context, though not directly referenced in current implementation)
- `tlist`: Target list to compare against the tuple descriptor
- `varno`: Variable number that Var nodes in the target list should reference
- `tupdesc`: Tuple descriptor to compare against the target list

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md) (gets first element of target list)
  - [lnext](../l/lnext.md) (iterates through target list)
  - TupleDescAttr (macro to access tuple descriptor attributes)
  - IsA (checks if node is a Var)
  - lfirst (gets list cell content)
- Called from (representative examples):
  - [ExecConditionalAssignProjectionInfo](../E/ExecConditionalAssignProjectionInfo.md) (execUtils.c:561)

## Notes and Other Information
- Static function used internally within execUtils.c for projection optimization
- Critical for performance: enables elimination of unnecessary projection steps
- Handles edge cases like typmod variations that can occur in union operations
- Rejects tuple descriptors with dropped columns or missing values for safety
- Returns false immediately on any mismatch, making it efficient for early detection of projection necessity
- The typmod handling allows flexibility while maintaining type safety in union scenarios

## Simplified Source

```c
static bool tlist_matches_tupdesc(PlanState *ps, List *tlist, int varno, TupleDesc tupdesc)
{
    int numattrs = tupdesc->natts;
    ListCell *tlist_item = list_head(tlist);

    // Check each attribute in the tuple descriptor
    for (int attrno = 1; attrno <= numattrs; attrno++)
    {
        Form_pg_attribute att_tup = TupleDescAttr(tupdesc, attrno - 1);

        // Must have corresponding target list entry
        if (tlist_item == NULL)
            return false;  // Target list too short

        // Target list entry must be a simple Var node
        Var *var = (Var *) ((TargetEntry *) lfirst(tlist_item))->expr;
        if (!var || !IsA(var, Var))
            return false;  // Not a simple variable reference

        // Var must reference correct attribute number
        if (var->varattno != attrno)
            return false;  // Attributes out of order

        // Skip dropped columns and columns with missing values
        if (att_tup->attisdropped || att_tup->atthasmissing)
            return false;

        // Check type compatibility
        // Allow typmod -1 (unspecified) to match any specific typmod
        if (var->vartype != att_tup->atttypid ||
            (var->vartypmod != att_tup->atttypmod && var->vartypmod != -1))
            return false;  // Type mismatch

        tlist_item = lnext(tlist, tlist_item);
    }

    // Target list must not have extra entries
    if (tlist_item)
        return false;  // Target list too long

    return true;  // Perfect match - projection can be optimized away
}
```