# IsBinaryTidClause

## Location
src/backend/optimizer/path/tidpath.c: 76 - 129

## Overview
IsBinaryTidClause is a static function that determines whether a RestrictInfo represents a binary operation involving a CTID variable and a pseudoconstant.

## Definition


## Detailed Description
This function examines a RestrictInfo to check if it represents a clause of the form "CTID OP pseudoconstant" or "pseudoconstant OP CTID", where OP is a binary operation. It validates that one operand is a CTID variable belonging to the specified relation, while the other operand is a pseudoconstant (an expression that doesn't reference the relation and contains no volatile functions). This is essential for identifying clauses that can be used for TID-based access paths in query optimization.

## Parameters / Member Variables
- : A RestrictInfo structure containing the clause to be examined
- : A RelOptInfo structure representing the relation being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md) (checks if clause is an OpExpr)
  - list_length (gets list length)
  - linitial (gets first list element)
  - lsecond (gets second list element)
  - IsA (type checking macro)
  - [IsCTIDVar](IsCTIDVar.md) (checks if variable is CTID)
  - [bms_is_member](../b/bms_is_member.md) (checks bitmap membership)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile functions)
- Called from (representative examples):
  - [IsTidEqualClause](IsTidEqualClause.md)
  - [IsTidRangeClause](IsTidRangeClause.md)

## Notes and Other Information
The function performs several validation steps: ensures the clause is an OpExpr with exactly two arguments, identifies which argument (if any) is a CTID variable for the specified relation, and verifies that the other argument is a true pseudoconstant by checking that it doesn't reference the relation and contains no volatile functions. This careful validation ensures that only appropriate clauses are considered for TID-based optimization.