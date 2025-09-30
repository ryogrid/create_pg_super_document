# pull_up_sublinks_qual_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:637-886](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L637-L886)

## Overview
Recursively processes qualification clauses to identify and transform SubLinks (ANY and EXISTS) into semijoin or anti-semijoin operations.

## Definition

```c
static Node *
pull_up_sublinks_qual_recurse(PlannerInfo *root, Node *node,
							  Node **jtlink1, Relids available_rels1,
							  Node **jtlink2, Relids available_rels2)
```
## Detailed Description
This function is the core engine for SubLink transformation during query optimization. It recursively traverses qualification expressions to find SubLink nodes that can be converted into more efficient join operations. The function handles several types of SubLink transformations:

**ANY SubLink Processing**: Converts clauses like "expr op ANY (subquery)" into semijoin operations using convert_ANY_sublink_to_join. The transformation preserves the original semantics while allowing the optimizer to consider join-based execution strategies.

**EXISTS SubLink Processing**: Transforms "EXISTS (subquery)" clauses into semijoins and "NOT EXISTS (subquery)" into anti-semijoins using convert_EXISTS_sublink_to_join.

**Dual Location Support**: The function supports two possible attachment points for pulled-up joins (jtlink1/available_rels1 and jtlink2/available_rels2), allowing flexibility in join placement based on variable references in the SubLink.

**AND Clause Recursion**: Recursively processes AND clauses to find SubLinks at any depth, while stopping at non-AND expressions to preserve proper semantics.

**NOT Clause Handling**: Special handling for "NOT EXISTS" patterns, where the NOT is recognized and passed to the conversion function to create anti-semijoins.

After successful SubLink conversion, the function:
1. Inserts the new JoinExpr into the appropriate location in the jointree
2. Recursively processes the pulled-up subtree with pull_up_sublinks_jointree_recurse
3. Recursively processes any remaining quals in the pulled-up subquery
4. Returns NULL (representing constant TRUE) since the condition is now handled by the join

## Parameters / Member Variables
- : PlannerInfo containing query optimization context and metadata
- : The qualification expression node to process (SubLink, BoolExpr, etc.)
- : Pointer to primary jointree location where new joins should be inserted
- : Set of relation IDs available at jtlink1 location
- : Pointer to secondary jointree location (optional, can be NULL)
- : Set of relation IDs available at jtlink2 location (optional)

## Dependencies
- Functions called/Symbols referenced:
  - [convert_ANY_sublink_to_join](../c/convert_ANY_sublink_to_join.md)
  - [convert_EXISTS_sublink_to_join](../c/convert_EXISTS_sublink_to_join.md)  
  - [pull_up_sublinks_jointree_recurse](pull_up_sublinks_jointree_recurse.md)
  - [is_notclause](../i/is_notclause.md), get_notclausearg
  - [is_andclause](../i/is_andclause.md), make_andclause
  - [lappend](../l/lappend.md), lfirst, linitial, list_length
  - IsA macro
  - ANY_SUBLINK, EXISTS_SUBLINK constants
- Called from (representative examples):
  - [pull_up_sublinks_jointree_recurse](pull_up_sublinks_jointree_recurse.md) (in multiple locations for processing join and fromexpr quals)
  - Self-recursive calls for processing AND clauses and pulled-up subquery quals

## Notes and Other Information
- Returns NULL when SubLink is successfully converted, representing constant TRUE
- Maintains original expression structure when SubLinks cannot be converted
- Handles complex nesting scenarios with proper variable scope management
- Critical for subquery decorrelation and semijoin optimization performance
- Must preserve NULL semantics of original SubLink expressions
- Stops recursion at non-AND expressions to avoid semantic changes
- Under NOT clauses, restricts pull-up to only reference the right-hand side of newly created joins
- Supports stacking multiple pulled-up SubLinks in encounter order, relying on subsequent optimization for reordering

## Simplified Source

```c
static Node *
pull_up_sublinks_qual_recurse(PlannerInfo *root, Node *node,
                              Node **jtlink1, Relids available_rels1,
                              Node **jtlink2, Relids available_rels2)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, SubLink))
    {
        SubLink *sublink = (SubLink *) node;
        JoinExpr *j;
        Relids child_rels;

        // Try to convert ANY SubLink (e.g., "expr IN (subquery)")
        if (sublink->subLinkType == ANY_SUBLINK)
        {
            // Try conversion with first available relation set
            if ((j = convert_ANY_sublink_to_join(root, sublink, available_rels1)) != NULL)
            {
                return handle_successful_conversion(root, j, jtlink1, available_rels1);
            }
            // Try conversion with second available relation set
            if (available_rels2 != NULL &&
                (j = convert_ANY_sublink_to_join(root, sublink, available_rels2)) != NULL)
            {
                return handle_successful_conversion(root, j, jtlink2, available_rels2);
            }
        }
        // Try to convert EXISTS SubLink (e.g., "EXISTS (subquery)")
        else if (sublink->subLinkType == EXISTS_SUBLINK)
        {
            // Try conversion with first available relation set
            if ((j = convert_EXISTS_sublink_to_join(root, sublink, false, available_rels1)) != NULL)
            {
                return handle_successful_conversion(root, j, jtlink1, available_rels1);
            }
            // Try conversion with second available relation set
            if (available_rels2 != NULL &&
                (j = convert_EXISTS_sublink_to_join(root, sublink, false, available_rels2)) != NULL)
            {
                return handle_successful_conversion(root, j, jtlink2, available_rels2);
            }
        }

        // Cannot convert - return unchanged
        return node;
    }

    if (is_notclause(node))
    {
        // Handle "NOT EXISTS" patterns - convert to anti-semijoin
        SubLink *sublink = (SubLink *) get_notclausearg((Expr *) node);
        JoinExpr *j;

        if (sublink && IsA(sublink, SubLink) && sublink->subLinkType == EXISTS_SUBLINK)
        {
            // Try anti-semijoin conversion (under_not = true)
            if ((j = convert_EXISTS_sublink_to_join(root, sublink, true, available_rels1)) != NULL)
            {
                return handle_not_exists_conversion(root, j, jtlink1, available_rels1);
            }
            if (available_rels2 != NULL &&
                (j = convert_EXISTS_sublink_to_join(root, sublink, true, available_rels2)) != NULL)
            {
                return handle_not_exists_conversion(root, j, jtlink2, available_rels2);
            }
        }

        return node;
    }

    if (is_andclause(node))
    {
        // Recursively process all clauses in AND expression
        List *newclauses = NIL;

        foreach(l, ((BoolExpr *) node)->args)
        {
            Node *oldclause = (Node *) lfirst(l);
            Node *newclause = pull_up_sublinks_qual_recurse(root, oldclause,
                                                           jtlink1, available_rels1,
                                                           jtlink2, available_rels2);
            if (newclause)
                newclauses = lappend(newclauses, newclause);
        }

        // Reconstruct AND clause with remaining clauses
        if (newclauses == NIL)
            return NULL;  // All clauses were converted
        else if (list_length(newclauses) == 1)
            return (Node *) linitial(newclauses);
        else
            return (Node *) make_andclause(newclauses);
    }

    // Not a convertible expression - return unchanged
    return node;
}

// Helper function for successful SubLink conversion
static Node *
handle_successful_conversion(PlannerInfo *root, JoinExpr *j, Node **jtlink, Relids available_rels)
{
    Relids child_rels;

    // Insert new join into jointree
    j->larg = *jtlink;
    *jtlink = (Node *) j;

    // Process pulled-up subtree
    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

    // Recursively process any remaining quals
    j->quals = pull_up_sublinks_qual_recurse(root, j->quals,
                                            &j->larg, available_rels,
                                            &j->rarg, child_rels);

    return NULL;  // SubLink converted - return constant TRUE
}

// Helper function for NOT EXISTS conversion
static Node *
handle_not_exists_conversion(PlannerInfo *root, JoinExpr *j, Node **jtlink, Relids available_rels)
{
    Relids child_rels;

    // Insert new join into jointree
    j->larg = *jtlink;
    *jtlink = (Node *) j;

    // Process pulled-up subtree
    j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &child_rels);

    // Under NOT, can only pull up sublinks referencing j->rarg
    j->quals = pull_up_sublinks_qual_recurse(root, j->quals,
                                            &j->rarg, child_rels,
                                            NULL, NULL);

    return NULL;  // SubLink converted - return constant TRUE
}
```