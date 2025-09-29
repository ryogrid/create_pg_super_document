# assign_collations_walker

## Location
[src/backend/parser/parse_collate.c:255-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L255-L779)

## Overview
The core recursive function that traverses an expression tree to assign collation information to all nodes based on their types and child expressions.

## Definition

```c
static bool
assign_collations_walker(Node *node, assign_collations_context *context)
```
## Detailed Description
This function is the recursive workhorse of PostgreSQL's collation assignment system. It walks through every node in an expression tree, determining the appropriate collation for each node based on several factors:

1. **Node Type Analysis**: Different node types have different collation inheritance rules (COLLATE expressions, field selections, aggregates, etc.)
2. **Child Collation Merging**: Combines collation information from child nodes using 
3. **Type System Integration**: Uses the PostgreSQL type system to determine if a node's result type is collatable
4. **Conflict Detection**: Identifies and reports collation conflicts where incompatible collations would be required

The function handles special cases for complex node types like aggregates (calling specialized functions like ), CASE expressions, and row comparisons. For most nodes, it follows a standard pattern: recurse to children, determine the node's collation based on type and child collations, then merge the result into the parent context.

## Parameters / Member Variables
- : The current expression node being processed (can be NULL for empty subexpressions)
- : Collation context containing state information including parser state and accumulated collation information

## Dependencies
- Functions called/Symbols referenced:
  -  (for recursive traversal)
  -  (for combining collation states)
  -  (for normal aggregates)
  -  (for ordered set aggregates)  
  -  (for hypothetical aggregates)
  - , ,  (collation accessors)
  -  (type system integration)
- Called from (representative examples):
  -  (entry point for expression collation assignment)
  - Itself (recursive calls for tree traversal)
  - Various aggregate collation assignment functions

## Notes and Other Information
- The function uses a local context for each recursion level to track collation state independently
- Special handling exists for nodes that don't contribute to parent collation (like RowExpr, join nodes)
- Error reporting includes source location information to help users identify collation conflicts
- The function sets both result collation and input collation on nodes, as functions may need different collation information for their inputs vs outputs

## Simplified Source

```c
static bool
assign_collations_walker(Node *node, assign_collations_context *context)
{
    assign_collations_context loccontext;
    Oid collation;
    CollateStrength strength;
    int location;

    // Handle null nodes
    if (node == NULL)
        return false;

    // Initialize local context for this recursion level
    loccontext.pstate = context->pstate;
    loccontext.collation = InvalidOid;
    loccontext.strength = COLLATE_NONE;
    loccontext.location = -1;

    // Process node based on its type
    switch (nodeTag(node))
    {
        case T_CollateExpr:
            // COLLATE clause sets explicit collation
            expression_tree_walker(node, assign_collations_walker, &loccontext);
            collation = ((CollateExpr *) node)->collOid;
            strength = COLLATE_EXPLICIT;
            location = ((CollateExpr *) node)->location;
            break;

        case T_FieldSelect:
            // Field selection uses the field's declared collation
            expression_tree_walker(node, assign_collations_walker, &loccontext);
            collation = ((FieldSelect *) node)->resultcollid;
            if (OidIsValid(collation)) {
                strength = COLLATE_IMPLICIT;
                location = exprLocation(node);
            } else {
                strength = COLLATE_NONE;
                location = -1;
            }
            break;

        case T_RowExpr:
            // Row expressions don't have collations
            assign_list_collations(context->pstate, ((RowExpr *) node)->args);
            return false;

        case T_Var:
        case T_Const:
        case T_Param:
            // Leaf nodes should already have collation assigned
            collation = exprCollation(node);
            strength = OidIsValid(collation) ? COLLATE_IMPLICIT : COLLATE_NONE;
            location = exprLocation(node);
            break;

        default:
            // General case: recurse to children, then assign based on type
            {
                Oid typcollation;

                // Special handling for complex nodes like aggregates and CASE
                switch (nodeTag(node))
                {
                    case T_Aggref:
                        assign_aggregate_collations((Aggref *) node, &loccontext);
                        break;
                    case T_CaseExpr:
                        assign_case_collations((CaseExpr *) node, &loccontext);
                        break;
                    default:
                        // Normal case: all children contribute equally
                        expression_tree_walker(node, assign_collations_walker, &loccontext);
                        break;
                }

                // Determine collation based on result type and children
                typcollation = get_typcollation(exprType(node));
                if (OidIsValid(typcollation)) {
                    if (loccontext.strength > COLLATE_NONE) {
                        // Bubble up from children
                        collation = loccontext.collation;
                        strength = loccontext.strength;
                        location = loccontext.location;
                    } else {
                        // Use type's default collation
                        collation = typcollation;
                        strength = COLLATE_IMPLICIT;
                        location = exprLocation(node);
                    }
                } else {
                    // Non-collatable type
                    collation = InvalidOid;
                    strength = COLLATE_NONE;
                    location = -1;
                }

                // Save collation info to the node
                exprSetCollation(node, strength == COLLATE_CONFLICT ? InvalidOid : collation);
                exprSetInputCollation(node, loccontext.strength == COLLATE_CONFLICT ?
                                           InvalidOid : loccontext.collation);
            }
            break;
    }

    // Merge this node's collation state into parent context
    merge_collation_state(collation, strength, location,
                         loccontext.collation2, loccontext.location2,
                         context);

    return false;
}
```