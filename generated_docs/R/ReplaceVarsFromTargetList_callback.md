# ReplaceVarsFromTargetList_callback

## Location
src/backend/rewrite/rewriteManip.c: 1669 - 1773

## Overview
A callback function used with replace_rte_variables to substitute Var nodes with corresponding expressions from a target list, handling whole-tuple references and various no-match scenarios.

## Definition


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
- : The Var node to be replaced
- : Contains the callback argument with target list and replacement options

## Dependencies
- Functions called/Symbols referenced:
  - ReplaceVarsFromTargetList_context (struct)
  - expandRTE
  - replace_rte_variables_mutator
  - get_tle_by_resno
  - copyObject
  - IncrementVarSublevelsUp
  - contains_multiexpr_param
  - get_typlenbyval
  - coerce_null_to_domain
  - RowExpr (node type)
  - InvalidAttrNumber (constant)
  - Various REPLACEVARS_* constants
- Called from (representative examples):
  - ReplaceVarsFromTargetList

## Notes and Other Information
- This is a static function, only used within rewriteManip.c as a callback
- Handles both named rowtypes and RECORD types differently for whole-tuple expansion
- Properly maintains column names for RECORD type expansions for executor and ruleutils usage
- Includes domain constraint handling when substituting NULL values
- Prevents semantic issues with multiple assignment parameters in ON UPDATE rules
- Recursive calls to replace_rte_variables_mutator ensure proper processing of expanded fields
- Careful sublevel adjustment maintains proper variable scoping across query levels