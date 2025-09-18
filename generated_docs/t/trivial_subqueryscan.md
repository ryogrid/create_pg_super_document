# trivial_subqueryscan

## Location
src/backend/optimizer/plan/setrefs.c: 1464 - 1533

## Overview
Determines whether a SubqueryScan node can be safely eliminated from the plan tree by checking if it adds no meaningful processing beyond passing through its subplan's output.

## Definition


## Detailed Description
 is a key optimization function that identifies SubqueryScan nodes that can be eliminated from the execution plan. A SubqueryScan is considered "trivial" when it serves no purpose other than wrapping its subplan - meaning it doesn't filter rows (no quals) and doesn't transform the output columns (targetlist just regurgitates subplan output).

The function implements caching to avoid repeated computation since it may be called multiple times during plan optimization phases. It uses the  field in the SubqueryScan node to track whether the determination has already been made.

The triviality check verifies:
1. No qualification conditions (plan.qual must be NIL)
2. Targetlist lengths match between parent and subplan
3. Each targetlist entry either:
   - Is a Var referencing the corresponding subplan output column in order
   - Is a Const that exactly equals the corresponding subplan constant expression
4. Junk status (resjunk) matches between corresponding entries

The function supports scenarios where targetlist entries are constants rather than just variables, which is important for set operations (see  for context).

## Parameters / Member Variables
- : The SubqueryScan node to evaluate for triviality

## Dependencies
- Functions called/Symbols referenced:
  - forboth: Macro for parallel iteration over two lists
  - equal: Tests equality between expression nodes
  - SUBQUERY_SCAN_TRIVIAL/NONTRIVIAL/UNKNOWN: Status enumeration values for caching results
- Called from (representative examples):
  - set_subqueryscan_references: Primary caller during plan reference adjustment
  - mark_async_capable_plan: Called during Append plan creation to determine async capability

## Notes and Other Information
The caching mechanism is particularly important because the function may be called from  before plan finalization and again from  during reference adjustment. The comments explain why this is safe - the transformations that occur between these calls preserve the properties that affect triviality determination. This optimization is crucial for query performance as it can eliminate entire plan nodes from execution, reducing tuple passing overhead and simplifying the execution tree. The support for Const expressions in addition to Vars makes the function robust for set operations where constant folding may have occurred.