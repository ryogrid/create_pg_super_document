# dependency_is_compatible_expression

## Location
src/backend/statistics/dependencies.c: 1168 - 1369

## Overview
Determines if an expression is compatible with functional dependencies by checking if it matches a statistics expression in the provided statistics list, extending beyond simple Var expressions to support complex expressions.

## Definition


## Detailed Description
This function serves as an extended version of  that supports complex expressions beyond simple Var nodes. It evaluates whether a clause can be used with functional dependencies by checking if the clause's expression matches any of the expressions tracked in extended statistics.

The function handles the same clause types as :
- **OpExpr**:  or  patterns
- **ScalarArrayOpExpr**:  with ANY semantics
- **OR clauses**: Recursively processes all sub-clauses ensuring they reference the same expression
- **NOT clauses**: Interprets  as 
- **Boolean expressions**: Interprets bare boolean  as 

The key difference is that instead of requiring a simple Var, this function searches through the provided statistics list to find a matching expression. This enables functional dependency usage with computed expressions like , , etc., that have extended statistics collected on them.

## Parameters / Member Variables
- : The clause node to examine for compatibility
- : The relation index that the clause should reference  
- : List of StatisticExtInfo structures containing tracked expressions
- : Output parameter that receives the matching statistics expression on success

## Dependencies
- Functions called/Symbols referenced:
  - bms_membership
  - is_opclause
  - is_pseudo_constant_clause
  - get_oprrest
  - is_orclause
  - is_notclause
  - get_notclausearg
  - equal (for expression comparison)
- Types used:
  - StatisticExtInfo
  - STATS_EXT_DEPENDENCIES
- Called from (representative examples):
  - DependencyGenerator
  - dependency_is_compatible_expression (recursive call for OR clauses)
  - dependencies_clauselist_selectivity

## Notes and Other Information
- Extends compatibility checking beyond simple Var expressions to complex expressions tracked in extended statistics
- Maintains the same clause validation logic as 
- Uses expression equality () to match clause expressions with statistics expressions
- Requires that expressions be tracked in extended statistics with dependency information
- For OR clauses, ensures all sub-expressions are identical using  comparison
- Enables functional dependency optimization for computed columns and expression indexes
- The function is recursive when processing OR clauses to ensure all sub-expressions match the same statistics expression