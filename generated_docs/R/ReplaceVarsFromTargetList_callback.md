# ReplaceVarsFromTargetList_callback

## Location
[src/backend/rewrite/rewriteManip.c:1669-1773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteManip.c#L1669-L1773)

## Overview
A callback function used with replace_rte_variables to substitute Var nodes with corresponding expressions from a target list, handling whole-tuple references and various no-match scenarios.

## Definition

```c
static Node *
ReplaceVarsFromTargetList_callback(Var *var,
								   replace_rte_variables_context *context)
```
## Detailed Description
This callback function implements the core logic for replacing variables with expressions from a target list. It handles several complex scenarios:

1. **Whole-tuple references (varattno = InvalidAttrNumber)**: Expands whole-row variables into RowExpr nodes containing all columns from the target relation. The expansion behavior differs based on whether the variable is of a named rowtype (plain relation) or RECORD type (JOIN), with different handling for dropped columns.

2. **Normal column references**: Looks up the target list entry by column number and returns a copy of the corresponding expression, with proper adjustment of variable sublevel references.

3. **No-match handling**: Provides three strategies when a column cannot be found:
   - REPLACEVARS_REPORT_ERROR: Raises an error
   - REPLACEVARS_CHANGE_VARNO: Changes the variable to reference a different RTE
   - REPLACEVARS_SUBSTITUTE_NULL: Replaces with a properly-typed NULL value

4. **Special error cases**: Detects and prevents the use of PARAM_MULTIEXPR parameters in ON UPDATE rules, which would create semantic complications.

## Parameters / Member Variables
- `*var`: The Var node to be replaced
- `*context`: Contains the callback argument with target list and replacement options
## Dependencies
- Functions called/Symbols referenced:
  - ReplaceVarsFromTargetList_context (struct)
  - [expandRTE](../e/expandRTE.md)
  - [replace_rte_variables_mutator](../r/replace_rte_variables_mutator.md)
  - [get_tle_by_resno](../g/get_tle_by_resno.md)
  - copyObject
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md)
  - [contains_multiexpr_param](../c/contains_multiexpr_param.md)
  - [get_typlenbyval](../g/get_typlenbyval.md)
  - [coerce_null_to_domain](../c/coerce_null_to_domain.md)
  - RowExpr (node type)
  - InvalidAttrNumber (constant)
  - Various REPLACEVARS_* constants
- Called from (representative examples):
  - [ReplaceVarsFromTargetList](ReplaceVarsFromTargetList.md)

## Notes and Other Information
- This is a static function, only used within rewriteManip.c as a callback
- Handles both named rowtypes and RECORD types differently for whole-tuple expansion
- Properly maintains column names for RECORD type expansions for executor and ruleutils usage
- Includes domain constraint handling when substituting NULL values
- Prevents semantic issues with multiple assignment parameters in ON UPDATE rules
- Recursive calls to replace_rte_variables_mutator ensure proper processing of expanded fields
- Careful sublevel adjustment maintains proper variable scoping across query levels

## Simplified Source

```c
static Node *
ReplaceVarsFromTargetList_callback(Var *var,
                                   replace_rte_variables_context *context)
{
    ReplaceVarsFromTargetList_context *rcon =
        (ReplaceVarsFromTargetList_context *) context->callback_arg;

    // Handle whole-tuple references (SELECT *)
    if (var->varattno == InvalidAttrNumber) {
        RowExpr *rowexpr;
        List *colnames;
        List *fields;

        // Expand relation into individual column references
        expandRTE(rcon->target_rte,
                  var->varno, var->varlevelsup, var->location,
                  (var->vartype != RECORDOID),
                  &colnames, &fields);

        // Apply replacements to the expanded fields
        fields = (List *) replace_rte_variables_mutator((Node *) fields, context);

        // Build RowExpr to represent the tuple
        rowexpr = makeNode(RowExpr);
        rowexpr->args = fields;
        rowexpr->row_typeid = var->vartype;
        rowexpr->row_format = COERCE_IMPLICIT_CAST;
        rowexpr->colnames = (var->vartype == RECORDOID) ? colnames : NIL;
        rowexpr->location = var->location;

        return (Node *) rowexpr;
    }

    // Normal case - lookup specific column in target list
    TargetEntry *tle = get_tle_by_resno(rcon->targetlist, var->varattno);

    if (tle == NULL || tle->resjunk) {
        // Handle column not found based on policy
        switch (rcon->nomatch_option) {
            case REPLACEVARS_CHANGE_VARNO:
                var = (Var *) copyObject(var);
                var->varno = rcon->nomatch_varno;
                return (Node *) var;

            case REPLACEVARS_SUBSTITUTE_NULL:
                // Create typed NULL with domain constraints
                return coerce_null_to_domain(var->vartype, var->vartypmod,
                                           var->varcollid, -1, false);

            default:
                elog(ERROR, "could not find replacement targetlist entry for attno %d",
                     var->varattno);
        }
    }

    // Found target entry - make a copy and adjust levels
    Expr *newnode = copyObject(tle->expr);

    if (var->varlevelsup > 0)
        IncrementVarSublevelsUp((Node *) newnode, var->varlevelsup, 0);

    // Check for unsupported multi-assignment parameters
    if (contains_multiexpr_param((Node *) newnode, NULL))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("NEW variables in ON UPDATE rules cannot reference "
                             "columns that are part of a multiple assignment")));

    return (Node *) newnode;
}
```