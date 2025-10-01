# replace_nestloop_param_placeholdervar

## Location
[src/backend/optimizer/util/paramassign.c:416-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/paramassign.c#L416-L479)

## Overview
Generates a Param node to replace a PlaceHolderVar that references a value from an outer NestLoop plan node, providing parameter management for placeholder variables with de-duplication support.

## Definition
```c
Param *replace_nestloop_param_placeholdervar(PlannerInfo *root, PlaceHolderVar *phv)
```

## Detailed Description
This function is the PlaceHolderVar equivalent of replace_nestloop_param_var, designed to handle placeholder variables that need to be passed from outer to inner relations in nested loop joins. PlaceHolderVars represent expressions that must be computed at specific join levels but may be referenced at different levels in the query plan.

The function follows the same de-duplication pattern as its Var counterpart:

1. **De-duplication Check**: Searches existing curOuterParams to find if an identical PlaceHolderVar has already been parameterized
2. **Reuse Existing Parameter**: If found, creates a Param node referencing the existing parameter slot, avoiding duplicate NestLoopParam entries
3. **Create New Parameter**: If not found, generates a new execution parameter and creates a corresponding NestLoopParam entry
4. **Type Information Extraction**: Uses the phexpr (placeholder expression) to determine type, type modifier, and collation information

A key difference from the Var version is that type information must be extracted from the PlaceHolderVar's contained expression (phexpr) rather than being directly available as struct members.

## Parameters / Member Variables
- `root`: PlannerInfo pointer representing the current query planning context, containing the curOuterParams list for parameter tracking
- `phv`: PlaceHolderVar pointer to the placeholder variable expression that needs to be parameterized for nested loop access

## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md): Tests structural equality between the input PlaceHolderVar and existing paramval entries
  - makeNode: Creates new Param and NestLoopParam nodes
  - [exprType](../e/exprType.md): Extracts the data type from the PlaceHolderVar's phexpr
  - [exprTypmod](../e/exprTypmod.md): Extracts the type modifier from the PlaceHolderVar's phexpr  
  - [exprCollation](../e/exprCollation.md): Extracts the collation information from the PlaceHolderVar's phexpr
  - [generate_new_exec_param](../g/generate_new_exec_param.md): Allocates a new execution parameter slot with proper type information
  - copyObject: Creates a deep copy of the PlaceHolderVar for storage in the NestLoopParam
  - [lappend](../l/lappend.md): Adds the new NestLoopParam to the curOuterParams list

- Called from (representative examples):
  - [replace_nestloop_params_mutator](replace_nestloop_params_mutator.md): Used during plan tree creation to parameterize PlaceHolderVars in nested loop contexts

## Notes and Other Information
- Functionally identical to replace_nestloop_param_var but specialized for PlaceHolderVar expressions
- Implements the same de-duplication mechanism using the curOuterParams list
- Type information is derived from the PlaceHolderVar's phexpr field rather than direct struct members
- Uses PARAM_EXEC parameter kind for execution-time parameter evaluation
- Location is set to -1 as PlaceHolderVars may not have meaningful source locations
- The paramval in NestLoopParam is cast to Var* but actually stores the PlaceHolderVar copy
- Critical for handling complex expressions that span multiple join levels in nested loop joins
- PlaceHolderVars often arise from subquery flattening and join reordering optimizations

## Simplified Source

```c
Param *replace_nestloop_param_placeholdervar(PlannerInfo *root, PlaceHolderVar *phv) {
    // Check if this PlaceHolderVar is already parameterized
    ListCell *lc;
    foreach(lc, root->curOuterParams) {
        NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
        if (equal(phv, nlp->paramval)) {
            // Reuse existing parameter slot
            Param *param = makeNode(Param);
            param->paramkind = PARAM_EXEC;
            param->paramid = nlp->paramno;
            param->paramtype = exprType((Node *) phv->phexpr);
            param->paramtypmod = exprTypmod((Node *) phv->phexpr);
            param->paramcollid = exprCollation((Node *) phv->phexpr);
            param->location = -1;
            return param;
        }
    }

    // Create new execution parameter
    Param *param = generate_new_exec_param(root,
                                         exprType((Node *) phv->phexpr),
                                         exprTypmod((Node *) phv->phexpr),
                                         exprCollation((Node *) phv->phexpr));

    // Add new NestLoopParam to track this parameterization
    NestLoopParam *nlp = makeNode(NestLoopParam);
    nlp->paramno = param->paramid;
    nlp->paramval = (Var *) copyObject(phv);
    root->curOuterParams = lappend(root->curOuterParams, nlp);

    return param;
}
```