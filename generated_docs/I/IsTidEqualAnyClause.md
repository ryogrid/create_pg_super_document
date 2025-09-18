# IsTidEqualAnyClause

## Location
src/backend/optimizer/path/tidpath.c: 172 - 210

## Overview
IsTidEqualAnyClause is a static function that identifies whether a RestrictInfo represents a CTID equality clause using the ANY operator for matching against multiple values.

## Definition


## Detailed Description
This function determines if a RestrictInfo represents a clause of the form "CTID = ANY (pseudoconstant_array)", where the CTID variable belongs to the specified relation and the array contains values that don't reference the relation. It validates that the clause is a ScalarArrayOpExpr using the TID equality operator with useOr=true, ensures the first argument is a CTID variable for the target relation, and verifies that the second argument (the array) is a pseudoconstant expression. This enables the optimizer to consider TID-based access when multiple specific tuple identifiers are being sought.

## Parameters / Member Variables
- : A PlannerInfo structure containing planner state and context
- : A RestrictInfo structure containing the clause to be examined
- : A RelOptInfo structure representing the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - IsA (type checking macro)
  - ScalarArrayOpExpr (scalar array operation expression type)
  - TIDEqualOperator (constant for TID equality operator)
  - list_length (gets list length)
  - linitial (gets first list element)
  - lsecond (gets second list element)
  - IsCTIDVar (checks if variable is CTID)
  - bms_is_member (checks bitmap membership)
  - pull_varnos (extracts variable numbers from expression)
  - contain_volatile_functions (checks for volatile functions)
- Called from (representative examples):
  - RestrictInfoIsTidQual

## Notes and Other Information
Unlike the other TID clause functions, this one handles the ANY operator which allows matching against multiple TID values in a single clause. It requires the useOr flag to be true (indicating OR semantics for the array elements) and specifically checks that the CTID variable is the first argument. The function uses pull_varnos to ensure the array expression doesn't reference the target relation, maintaining the pseudoconstant requirement essential for TID-based optimization.