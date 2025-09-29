# process_matched_tle

## Location
[src/backend/rewrite/rewriteHandler.c:1036-1188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1036-L1188)

## Overview
Converts a matched TargetEntry from the original target list into a correct new TargetEntry, specifically handling multiple assignments to the same target attribute.

## Definition

```c
structure and just consider the top
	 * FieldStore or SubscriptingRef from each assignment, because it works to
	 * combine these as
	 *		FieldStore(FieldStore(col, fld1,
	 *							  FieldStore(placeholder, subfld1, x)),
	 *				   fld2, FieldStore(placeholder, subfld2, y))
	 * Note the leftmost expression goes on the inside so that the
	 * assignments appear to occur left-to-right.
	 *
	 * For FieldStore, instead of nesting we can generate a single
	 * FieldStore with multiple target fields.  We must nest when
	 * SubscriptingRefs are involved though.
	 *
	 * As a further complication, the destination column might be a domain,
	 * resulting in each assignment containing a CoerceToDomain node over a
	 * FieldStore or SubscriptingRef.  These should have matching target
	 * domains, so we strip them and reconstitute a single CoerceToDomain over
	 * the combined FieldStore/SubscriptingRef nodes.  (Notice that this has
	 * the result that the domain's checks are applied only after we do all
	 * the field or element updates, not after each one.  This is desirable.)
	 *----------
	 */
	src_expr = (Node *) src_tle->expr;
```
## Detailed Description
This function is a critical component of PostgreSQL's rewrite system that handles complex UPDATE operations involving multiple assignments to the same column attribute. It intelligently combines FieldStore and SubscriptingRef operations when multiple assignments target the same attribute (e.g., ).

The function implements sophisticated logic to:
- Detect and validate multiple assignments to the same attribute
- Handle nested FieldStore and SubscriptingRef operations
- Manage CoerceToDomain nodes for domain-typed columns
- Combine multiple FieldStore operations into a single operation when possible
- Preserve assignment order (left-to-right execution)

For domain-typed columns, it strips CoerceToDomain nodes during processing and reconstitutes a single CoerceToDomain over the combined operations, ensuring domain checks are applied only after all field/element updates are complete.

## Parameters / Member Variables
- : The current TargetEntry being processed from the source target list
- : Previously processed TargetEntry for the same attribute (NULL if this is the first assignment)
- : The name of the target attribute (used only for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - [get_assignment_input](../g/get_assignment_input.md)
  - [CoerceToDomain](../C/CoerceToDomain.md)
  - [FieldStore](../F/FieldStore.md)
  - [SubscriptingRef](../S/SubscriptingRef.md)
  - [equal](../e/equal.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [flatCopyTargetEntry](../f/flatCopyTargetEntry.md)
- Called from (representative examples):
  - [rewriteTargetListIU](../r/rewriteTargetListIU.md)

## Notes and Other Information
- Only allows multiple assignments if all are FieldStore or SubscriptingRef operations
- For FieldStore operations, combines multiple targets into a single FieldStore when possible
- For SubscriptingRef operations, nesting is always required
- Domain constraint checking is deferred until after all field updates are complete
- Generates syntax errors for incompatible multiple assignments to prevent data corruption

## Simplified Source

```c
static TargetEntry *
process_matched_tle(TargetEntry *src_tle,
                    TargetEntry *prior_tle,
                    const char *attrName)
{
    TargetEntry *result;
    Node *src_expr, *prior_expr;
    Node *newexpr;
    CoerceToDomain *coerce_expr = NULL;

    // First assignment to this attribute - just return it
    if (prior_tle == NULL)
        return src_tle;

    // Handle multiple assignments to same attribute
    src_expr = (Node *) src_tle->expr;
    prior_expr = (Node *) prior_tle->expr;

    // Extract any CoerceToDomain wrappers
    if (IsA(src_expr, CoerceToDomain) && IsA(prior_expr, CoerceToDomain))
    {
        coerce_expr = (CoerceToDomain *) src_expr;
        src_expr = (Node *) coerce_expr->arg;
        prior_expr = (Node *) ((CoerceToDomain *) prior_expr)->arg;
    }

    // Validate that both expressions are compatible assignment operations
    Node *src_input = get_assignment_input(src_expr);
    Node *prior_input = get_assignment_input(prior_expr);

    if (src_input == NULL || prior_input == NULL)
        ereport(ERROR, (errmsg("multiple assignments to same column \"%s\"", attrName)));

    // Find the bottom-level input reference and validate compatibility
    Node *priorbottom = prior_input;
    while (get_assignment_input(priorbottom) != NULL)
        priorbottom = get_assignment_input(priorbottom);

    if (!equal(priorbottom, src_input))
        ereport(ERROR, (errmsg("multiple assignments to same column \"%s\"", attrName)));

    // Combine the assignments based on type
    if (IsA(src_expr, FieldStore))
    {
        FieldStore *fstore = makeNode(FieldStore);

        if (IsA(prior_expr, FieldStore))
        {
            // Combine two FieldStores into single operation
            memcpy(fstore, prior_expr, sizeof(FieldStore));
            fstore->newvals = list_concat_copy(
                ((FieldStore *) prior_expr)->newvals,
                ((FieldStore *) src_expr)->newvals);
            fstore->fieldnums = list_concat_copy(
                ((FieldStore *) prior_expr)->fieldnums,
                ((FieldStore *) src_expr)->fieldnums);
        }
        else
        {
            // Nest new FieldStore over prior expression
            memcpy(fstore, src_expr, sizeof(FieldStore));
            fstore->arg = (Expr *) prior_expr;
        }
        newexpr = (Node *) fstore;
    }
    else if (IsA(src_expr, SubscriptingRef))
    {
        // Nest SubscriptingRef operations
        SubscriptingRef *sbsref = makeNode(SubscriptingRef);
        memcpy(sbsref, src_expr, sizeof(SubscriptingRef));
        sbsref->refexpr = (Expr *) prior_expr;
        newexpr = (Node *) sbsref;
    }
    else
    {
        elog(ERROR, "unsupported assignment operation");
    }

    // Restore CoerceToDomain wrapper if present
    if (coerce_expr)
    {
        CoerceToDomain *newcoerce = makeNode(CoerceToDomain);
        memcpy(newcoerce, coerce_expr, sizeof(CoerceToDomain));
        newcoerce->arg = (Expr *) newexpr;
        newexpr = (Node *) newcoerce;
    }

    // Create result TargetEntry with combined expression
    result = flatCopyTargetEntry(src_tle);
    result->expr = (Expr *) newexpr;
    return result;
}
```