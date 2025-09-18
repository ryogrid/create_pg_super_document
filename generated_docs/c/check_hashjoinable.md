# check_hashjoinable

## Location
src/backend/optimizer/plan/initsplan.c: 3411 - 3438

## Overview
Determines if a restriction clause is suitable for hash join operations and sets the hashjoin operator field in the RestrictInfo structure if applicable.

## Definition


## Detailed Description
This function evaluates whether a given restriction clause can be used in hash join operations, one of PostgreSQL's most important join algorithms. Hash joins work by building a hash table from the smaller relation (the "inner" relation) and then probing this hash table for each row in the larger relation (the "outer" relation). This makes hash joins particularly efficient for large datasets where one side is significantly smaller than the other, or when no useful indexes exist.

The function follows a similar validation pattern to check_mergejoinable:
1. Skips pseudoconstant clauses that don't involve variables from multiple relations
2. Ensures the clause is an operator expression (OpExpr) 
3. Verifies the operator is binary (has exactly two arguments)
4. Checks if the operator is hashjoinable using system catalog information
5. Verifies that no volatile functions are present (volatile functions would compromise the hash join's correctness)

Unlike merge joins, hash joins don't require sorted input data but do require the ability to compute hash values for the join keys. If all validation criteria are met, the function stores the operator OID in the hashjoinoperator field, enabling the query planner to consider hash join strategies.

## Parameters / Member Variables
- : RestrictInfo structure containing the clause to evaluate and the hashjoinoperator field to populate if the clause qualifies for hash joins

## Dependencies
- Functions called/Symbols referenced:
  - is_opclause (verifies expression is an operator clause)
  - op_hashjoinable (determines if operator supports hash joins)
  - contain_volatile_functions (checks for volatile function calls)
  - exprType (determines expression data type)
  - linitial (retrieves first list element)
  - OpExpr (operator expression node type)
- Called from:
  - distribute_restrictinfo_to_rels (during restriction info distribution)
  - build_implied_join_equality (when constructing implied equality conditions)

## Notes and Other Information
- This is a static function within initsplan.c, serving as an internal query planning utility
- Hash joins are memory-intensive operations that build hash tables, so the planner must consider available work memory
- Unlike merge joins, hash joins don't benefit from pre-sorted data but can handle unsorted inputs efficiently
- The exclusion of volatile functions is crucial because hash joins may not evaluate the join condition for all row combinations
- Hash joins are often preferred over nested loop joins for large datasets without useful indexes
- The hashjoinoperator field set by this function is used later in cost estimation and join algorithm selection
- Hash joins require hashable data types - not all data types support hash operations
- This function complements check_mergejoinable, giving the planner multiple join algorithm options for the same clause