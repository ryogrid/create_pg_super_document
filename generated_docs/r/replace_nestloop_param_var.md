# replace_nestloop_param_var

## Location
src/backend/optimizer/util/paramassign.c: 367 - 415

## Overview
Generates a Param node to replace a Var that references a value from an outer NestLoop plan node, managing parameter passing for nested loop joins with de-duplication support.

## Definition
```c
Param *replace_nestloop_param_var(PlannerInfo *root, Var *var)
```

## Detailed Description
This function is a key component of PostgreSQL's nested loop parameter management system. When a nested loop join needs to pass values from its outer relation to its inner relation, variables from the outer relation must be converted into parameters that can be accessed by the inner relation's plan.

The function implements an efficient de-duplication mechanism by maintaining a list of already-created NestLoopParam entries in root->curOuterParams. The process works as follows:

1. **De-duplication Check**: First searches the existing curOuterParams list to see if an identical Var has already been parameterized
2. **Reuse Existing Parameter**: If found, creates a new Param node referencing the existing parameter slot without creating duplicate NestLoopParam entries
3. **Create New Parameter**: If not found, generates a new execution parameter slot and creates a corresponding NestLoopParam entry
4. **Registration**: Adds the new NestLoopParam to the curOuterParams list for future de-duplication

This approach ensures that identical variables from the outer relation share the same parameter slot, optimizing both memory usage and execution efficiency.

## Parameters / Member Variables
- `root`: PlannerInfo pointer representing the current query planning context, containing the curOuterParams list for parameter tracking
- `var`: Var pointer to the variable expression that needs to be parameterized for nested loop access

## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md): Tests structural equality between the input Var and existing paramval entries
  - makeNode: Creates new Param and NestLoopParam nodes
  - [generate_new_exec_param](../g/generate_new_exec_param.md): Allocates a new execution parameter slot with proper type information
  - copyObject: Creates a deep copy of the Var for storage in the NestLoopParam
  - lappend: Adds the new NestLoopParam to the curOuterParams list

- Called from (representative examples):
  - [replace_nestloop_params_mutator](replace_nestloop_params_mutator.md): Used during plan tree creation to parameterize variables in nested loop contexts

## Notes and Other Information
- Implements de-duplication by checking existing curOuterParams before creating new parameter slots
- Uses PARAM_EXEC parameter kind, indicating execution-time parameter evaluation
- Preserves all type information (vartype, vartypmod, varcollid) from the original Var in the Param node
- Location information is preserved for error reporting and debugging purposes  
- The curOuterParams list serves as both a de-duplication cache and a specification for required NestLoop parameters
- Each NestLoopParam entry links a parameter number (paramno) with its source expression (paramval)
- Critical for nested loop join performance as it enables efficient parameter passing without redundant evaluations