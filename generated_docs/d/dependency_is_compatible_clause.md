# dependency_is_compatible_clause

## Location
src/backend/statistics/dependencies.c: 741 - 928

## Overview
Determines if a clause is compatible with functional dependencies by analyzing whether it represents an equality condition with a pseudoconstant that can be used for dependency-based selectivity estimation.

## Definition


## Detailed Description
This function examines a WHERE clause to determine if it's suitable for use with functional dependencies in selectivity estimation. The function accepts clauses that have the form of equality to a pseudoconstant, or can be interpreted that way. The variable part of the clause must be a simple Var belonging to the specified relation.

The function handles several types of clauses:
- **OpExpr**: Checks for  or  patterns using equality operators
- **ScalarArrayOpExpr**: Handles  expressions with ANY semantics
- **OR clauses**: Recursively processes OR expressions ensuring all sub-clauses reference the same attribute
- **NOT clauses**: Interprets  as 
- **Boolean expressions**: Interprets bare boolean  as 

The function validates that the operator used is an equality operator by checking if  returns , ensuring compatibility with functional dependency logic.

## Parameters / Member Variables
- : The clause node to examine for compatibility with functional dependencies
- : The relation index that the clause should reference
- : Output parameter that receives the attribute number of the variable on success

## Dependencies
- Functions called/Symbols referenced:
  - [bms_membership](../b/bms_membership.md)
  - [is_opclause](../i/is_opclause.md)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_notclause](../i/is_notclause.md)
  - [get_notclausearg](../g/get_notclausearg.md)
  - AttrNumberIsForUserDefinedAttr
- Called from (representative examples):
  - DependencyGenerator
  - [dependency_is_compatible_clause](dependency_is_compatible_clause.md) (recursive call for OR clauses)
  - [dependencies_clauselist_selectivity](dependencies_clauselist_selectivity.md)

## Notes and Other Information
- Only supports simple Var expressions, not complex expressions or functions
- Rejects pseudoconstant clauses since they cannot contain variables
- Ensures clauses reference only a single relation (singleton bitmap membership)
- Filters out system attributes as statistics are not maintained for them
- Uses a somewhat dubious method of checking equality operators via selectivity functions rather than btree/hash opclass membership
- The function is recursive when processing OR clauses to ensure all sub-clauses reference the same attribute