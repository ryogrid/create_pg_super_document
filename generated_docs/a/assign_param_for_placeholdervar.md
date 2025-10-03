# assign_param_for_placeholdervar

## Location
[src/backend/optimizer/util/paramassign.c:149-196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L149-L196)

## Overview
Selects a PARAM_EXEC number to identify the given PlaceHolderVar as a parameter for the current subquery and records the need for the PHV in the proper upper-level root->plan_params.

## Definition

```c
static int
assign_param_for_placeholdervar(PlannerInfo *root, PlaceHolderVar *phv)
```
## Detailed Description
This function is analogous to assign_param_for_var but specifically handles PlaceHolderVar nodes instead of Var nodes. PlaceHolderVars are special constructs used in PostgreSQL's optimizer to represent expressions that need to be evaluated at specific query levels, particularly in complex joins and subqueries.

The function navigates up the planner hierarchy to find the appropriate query level where the PlaceHolderVar belongs, then searches for an existing matching PlannerParamItem based on the PHV's unique identifier (phid). If no match is found, it creates a new parameter entry. The function uses IncrementVarSublevelsUp to adjust the PHV's level references and ensures the phlevelsup is set to 0 for the parameterized version.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state and context for the current query level
- `*phv`: PlaceHolderVar node representing a placeholder expression that needs to be parameterized
## Dependencies
- Functions called/Symbols referenced:
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (structure handling)
  - [PlannerParamItem](../P/PlannerParamItem.md) (structure creation)
  - copyObject (deep copy of the PlaceHolderVar node)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md) (adjust variable level references)
  - makeNode (node creation)
  - [lappend_oid](../l/lappend_oid.md) (append OID to list)
  - [exprType](../e/exprType.md) (get expression type)
- Called from (representative examples):
  - [replace_outer_placeholdervar](../r/replace_outer_placeholdervar.md)

## Notes and Other Information
- The function assumes that comparing PHIDs (PlaceHolderVar IDs) is sufficient for matching
- Uses IncrementVarSublevelsUp with negative phlevelsup to adjust level references
- Includes an assertion to ensure phlevelsup is 0 after adjustment  
- The parameter type is determined by calling exprType on the PHV's expression (phexpr)
- This is a static function within paramassign.c, used internally for PlaceHolderVar parameter assignment
- PlaceHolderVars are more complex than regular Vars as they can contain arbitrary expressions

## Simplified Source

```c
static int
assign_param_for_placeholdervar(PlannerInfo *root, PlaceHolderVar *phv)
{
    ListCell *ppl;
    PlannerParamItem *pitem;
    Index levelsup;

    // Navigate to the query level where this PHV belongs
    for (levelsup = phv->phlevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    // Check if we already have a parameter for this PHV
    foreach(ppl, root->plan_params)
    {
        pitem = (PlannerParamItem *) lfirst(ppl);
        if (IsA(pitem->item, PlaceHolderVar))
        {
            PlaceHolderVar *existing_phv = (PlaceHolderVar *) pitem->item;

            // Match by PHV unique identifier
            if (existing_phv->phid == phv->phid)
                return pitem->paramId;
        }
    }

    // Create new parameter entry for this PHV
    phv = copyObject(phv);

    // Adjust level references to make this PHV relative to current level
    IncrementVarSublevelsUp((Node *) phv, -((int) phv->phlevelsup), 0);
    Assert(phv->phlevelsup == 0);

    // Create and initialize new parameter item
    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node *) phv;
    pitem->paramId = list_length(root->glob->paramExecTypes);

    // Record parameter type and add to lists
    root->glob->paramExecTypes = lappend_oid(root->glob->paramExecTypes,
                                            exprType((Node *) phv->phexpr));
    root->plan_params = lappend(root->plan_params, pitem);

    return pitem->paramId;
}
```