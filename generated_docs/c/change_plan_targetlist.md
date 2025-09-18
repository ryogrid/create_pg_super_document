# change_plan_targetlist

## Location
src/backend/optimizer/plan/createplan.c: 2153 - 2180

## Overview
A wrapper function that modifies a plan's target list, optimizing the process by either directly updating the target list or injecting a Result node when projection capabilities are limited.

## Definition


## Detailed Description
The  function serves as an externally available wrapper for , designed specifically for use by Foreign Data Wrapper (FDW) plan-generation functions. It allows modification of the target list computed by some subplan tree while maintaining optimization where possible.

The function intelligently determines whether the subplan can handle the new target list directly or requires a Result node to perform the projection. If the top plan node cannot do projections and the existing target list differs from the desired one, it injects a projection plan. Otherwise, it simply replaces the plan node's target list directly, which is more efficient.

This optimization is crucial for query performance as it avoids unnecessary Result nodes when the underlying plan can handle the projection natively.

## Parameters / Member Variables
- : The input Plan node whose target list needs to be modified
- : The new target list (List of TargetEntry nodes) to be applied to the plan
- : Boolean flag indicating whether the new target list is safe for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if a plan node can perform projections
  - : Compares two target lists for expression equivalence
  - : Adds a Result node to handle projection when needed
- Called from (representative examples):
  - : Used in unique plan creation
  - Various FDW implementations (external usage)

## Notes and Other Information
- This function is specifically designed for FDW plan-generation functions but can be used by other components
- The  parameter is typically passed from the FDW's own Path node's parallel_safe flag
- The function maintains the parallel safety of the plan by performing a logical AND operation between the subplan's parallel safety and the new target list's parallel safety
- Performance optimization: Avoids creating unnecessary Result nodes when direct target list replacement is possible