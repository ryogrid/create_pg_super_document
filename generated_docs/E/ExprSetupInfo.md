# ExprSetupInfo

## Location
src/backend/executor/execExpr.c: 56 - 64

## Overview
ExprSetupInfo is a structure used during expression compilation to collect metadata about what tuple slot attributes and MULTIEXPR SubPlan nodes are needed before expression evaluation.

## Definition
```c
typedef struct ExprSetupInfo
{
    /* Highest attribute numbers fetched from inner/outer/scan tuple slots: */
    AttrNumber  last_inner;
    AttrNumber  last_outer;
    AttrNumber  last_scan;
    /* MULTIEXPR SubPlan nodes appearing in the expression: */
    List       *multiexpr_subplans;
} ExprSetupInfo;
```

## Detailed Description
ExprSetupInfo serves as a collection structure during the expression compilation process in PostgreSQL's executor. It is used to analyze an expression tree and determine what preparatory steps are needed before the expression can be evaluated. The structure tracks the highest-numbered attributes that will be accessed from different tuple slot types (inner, outer, and scan) and collects any MULTIEXPR SubPlan nodes that appear within the expression.

This information is gathered by walking the expression tree with `expr_setup_walker()` and is then used by `ExecPushExprSetupSteps()` to generate the appropriate setup steps in the expression state. The setup process ensures that the necessary tuple slot attributes are properly deformed and that MULTIEXPR SubPlans are correctly initialized before expression evaluation begins.

## Parameters / Member Variables
- `last_inner`: The highest attribute number that will be accessed from the inner tuple slot
- `last_outer`: The highest attribute number that will be accessed from the outer tuple slot  
- `last_scan`: The highest attribute number that will be accessed from the scan tuple slot
- `multiexpr_subplans`: A list of MULTIEXPR SubPlan nodes found within the expression tree

## Dependencies
- Functions called/Symbols referenced:
  - AttrNumber (type)
  - [List](../L/List.md) (type)
- Called from (representative examples):
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md)
  - [ExecPushExprSetupSteps](ExecPushExprSetupSteps.md)
  - [expr_setup_walker](../e/expr_setup_walker.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecBuildAggTrans](ExecBuildAggTrans.md)

## Notes and Other Information
- The structure is typically initialized with zero values: `{0, 0, 0, NIL}`
- The attribute numbers are tracked using the Max() function to ensure the highest-numbered attribute is recorded
- MULTIEXPR SubPlans require special setup handling and are collected separately from regular tuple slot attributes
- This metadata collection is part of PostgreSQL's expression compilation optimization, allowing the executor to pre-setup only the tuple slot attributes that are actually needed by the expression
- The structure is used internally during expression state building and is not exposed to external callers