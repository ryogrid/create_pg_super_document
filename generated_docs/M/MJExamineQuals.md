# MJExamineQuals

## Location
[src/backend/executor/nodeMergejoin.c:175-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMergejoin.c#L175-L293)

## Overview
Deconstructs the list of mergejoinable expressions and builds an array of MergeJoinClause structs containing comparison information needed at runtime for merge join execution.

## Definition


## Detailed Description
This function processes the mergejoinable expressions provided by the planner in the form of "leftexpr = rightexpr" expression trees. The expressions are ordered to match the sort columns of the input relations. For each merge clause, the function:

1. Initializes the left and right expressions for execution
2. Sets up sort support data structures with proper collation and ordering
3. Extracts operator family properties to validate equality operators
4. Obtains comparison functions from the operator family, preferring sort support functions over traditional btree comparison functions
5. Creates MergeJoinClause structs containing all necessary runtime comparison information

The function ensures that abbreviation optimization is disabled for merge joins since there's no convenient opportunity to convert to alternative representations during the merge process.

## Parameters / Member Variables
- : List of mergejoinable expression trees from the planner
- : Array of btree operator family OIDs for each merge key
- : Array of collation OIDs for each merge key  
- : Array of btree strategy numbers (BTLessStrategyNumber or BTGreaterStrategyNumber)
- : Array of nulls-first flags indicating null placement in sort order
- : Parent plan state node for expression initialization context

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - [get_opfamily_proc](../g/get_opfamily_proc.md)
  - OidFunctionCall1
  - PrepareSortSupportComparisonShim
  - lsecond
  - BTSORTSUPPORT_PROC
  - BTORDER_PROC
- Called from:
  - ExecInitMergeJoin

## Notes and Other Information
- The function is static and only used internally within the merge join executor
- Validates that all merge clauses are OpExpr nodes and use equality operators
- Prioritizes sort support functions over traditional comparison functions for better performance
- Sets up proper sort ordering (ascending/descending) and null handling based on planner specifications
- Memory allocation uses palloc0 to ensure proper initialization of the MergeJoinClause array