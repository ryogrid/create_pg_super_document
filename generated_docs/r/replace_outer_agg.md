# replace_outer_agg

## Location
[src/backend/optimizer/util/paramassign.c:224-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L224-L269)

## Overview
Generates a Param node to replace the given Aggref which is expected to have agglevelsup > 0, and records the need for the Aggref in the proper upper-level root->plan_params.

## Definition

```c
Param *
replace_outer_agg(PlannerInfo *root, Aggref *agg)
```
## Detailed Description
This function handles the parameterization of aggregate function references (Aggref nodes) that belong to outer query levels. Unlike the deduplication strategies used for Vars and PlaceHolderVars, this function intentionally creates a new parameter slot every time, as indicated by the comment that it does not seem worthwhile to try to de-duplicate references to outer aggregates.

The function navigates up the planner hierarchy to find the query level where the aggregate belongs, then creates a copy of the Aggref and adjusts its level references using IncrementVarSublevelsUp. It then creates both a PlannerParamItem to track the parameter and a Param node to replace the original Aggref. The resulting parameter has type information derived directly from the aggregate's type fields.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context for the current query level
- : Aggref node representing an aggregate function reference from an outer query level (agglevelsup > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [Aggref](../A/Aggref.md) (aggregate reference structure)
  - [Param](../P/Param.md) (parameter node structure)
  - [PlannerParamItem](../P/PlannerParamItem.md) (parameter item structure)
  - copyObject (deep copy of the Aggref node)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md) (adjust variable level references)
  - makeNode (node creation)
  - [lappend_oid](../l/lappend_oid.md) (append OID to list)
  - PARAM_EXEC (parameter type constant)
- Called from (representative examples):
  - [replace_correlation_vars_mutator](replace_correlation_vars_mutator.md)
  - PARAMASSIGN_H (header file reference)

## Notes and Other Information
- The function includes an assertion that agg->agglevelsup > 0 and agg->agglevelsup < root->query_level
- Unlike other parameter assignment functions, this one does not attempt deduplication and creates a new parameter slot each time
- Uses IncrementVarSublevelsUp with negative agglevelsup to adjust level references
- Includes an assertion to ensure agglevelsup is 0 after adjustment
- The paramtypmod is set to -1, indicating no specific type modifier
- Type information (aggtype, aggcollid) is copied directly from the Aggref
- Location information is preserved for error reporting purposes
- This function is declared in optimizer/paramassign.h and is part of the public interface for parameter assignment

## Simplified Source

```c
Param *replace_outer_agg(PlannerInfo *root, Aggref *agg) {
    Param *retval;
    PlannerParamItem *pitem;
    Index levelsup;

    // Validate that this is an outer aggregate reference
    Assert(agg->agglevelsup > 0 && agg->agglevelsup < root->query_level);

    // Navigate up to the query level where this aggregate belongs
    for (levelsup = agg->agglevelsup; levelsup > 0; levelsup--)
        root = root->parent_root;

    // Create a copy and adjust level references
    agg = copyObject(agg);
    IncrementVarSublevelsUp((Node *) agg, -((int) agg->agglevelsup), 0);
    Assert(agg->agglevelsup == 0);

    // Create parameter item to track this aggregate
    pitem = makeNode(PlannerParamItem);
    pitem->item = (Node *) agg;
    pitem->paramId = list_length(root->glob->paramExecTypes);
    root->glob->paramExecTypes = lappend_oid(root->glob->paramExecTypes, agg->aggtype);
    root->plan_params = lappend(root->plan_params, pitem);

    // Create the parameter node to replace the aggregate
    retval = makeNode(Param);
    retval->paramkind = PARAM_EXEC;
    retval->paramid = pitem->paramId;
    retval->paramtype = agg->aggtype;
    retval->paramtypmod = -1;
    retval->paramcollid = agg->aggcollid;
    retval->location = agg->location;

    return retval;
}
```