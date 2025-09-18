# MakeTidOpExpr

## Location
src/backend/executor/nodeTidrangescan.c: 57 - 105

## Overview
MakeTidOpExpr creates a TidOpExpr structure from an OpExpr for TID range scan operations, determining the appropriate boundary type and inclusiveness based on the operator and operand order.

## Definition


## Detailed Description
This static function processes an OpExpr containing a CTID (tuple identifier) comparison and converts it into a TidOpExpr structure suitable for TID range scanning. The function analyzes the operator type and operand positions to determine whether the expression represents an upper or lower bound for the TID range, and whether the bound should be inclusive or exclusive. It handles operator inversion when the CTID variable appears on the right side of the comparison.

## Parameters / Member Variables
- `expr`: OpExpr containing the CTID comparison operation to be converted
- `tidstate`: TidRangeScanState containing the executor state for the TID range scan

## Dependencies
- Functions called/Symbols referenced:
  - [get_leftop](../g/get_leftop.md)
  - [get_rightop](../g/get_rightop.md)
  - [IsCTIDVar](../I/IsCTIDVar.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [palloc](../p/palloc.md)
  - elog
- Constants used:
  - TIDLessEqOperator
  - TIDLessOperator
  - TIDGreaterEqOperator
  - TIDGreaterOperator
  - TIDEXPR_LOWER_BOUND
  - TIDEXPR_UPPER_BOUND
- Called from:
  - [TidExprListCreate](../T/TidExprListCreate.md)

## Notes and Other Information
- The function determines inclusiveness based on whether the operator includes equality (<=, >=)
- Operator inversion occurs when the CTID variable is on the right side of the comparison
- The function will throw an error if it cannot identify the CTID variable or operator
- Returns a newly allocated TidOpExpr structure with appropriate boundary settings