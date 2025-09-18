# contain_var_clause

## Location
[src/backend/optimizer/util/var.c:403-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L403-L408)

## Overview
Recursively scans a clause to discover whether it contains any Var nodes of the current query level.

## Definition


## Detailed Description
The  function performs a boolean test to determine if a given parse tree node or expression contains any variable references (Var nodes) at the current query level (level 0). This is a utility function commonly used throughout PostgreSQL's optimizer and other components to quickly check for variable dependencies.

The function serves as a simple wrapper around , which performs the actual tree traversal. It's designed to be used after sublinks have been reduced to subplans, as it does not examine subqueries.

This function is particularly useful for:
- Determining if expressions are truly constant
- Checking constraint definitions for variable references  
- Optimizing query plans by identifying variable-free expressions
- Validating partition bounds and function parameters

## Parameters / Member Variables
- : The root node of the parse tree or expression to examine

## Dependencies
- Functions called/Symbols referenced:
  - [contain_var_clause_walker](contain_var_clause_walker.md) (performs the actual tree walking)
- Called from (representative examples):
  - [cookDefault](cookDefault.md) (in src/backend/catalog/heap.c)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md) (in src/backend/commands/functioncmds.c)
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md) (in src/backend/commands/typecmds.c)
  - [match_clause_to_ordering_op](../m/match_clause_to_ordering_op.md) (in src/backend/optimizer/path/indxpath.c)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md) (in src/backend/optimizer/util/clauses.c)
  - [transformPartitionBoundValue](../t/transformPartitionBoundValue.md) (in src/backend/parser/parse_utilcmd.c)
  - [match_clause_to_partition_key](../m/match_clause_to_partition_key.md) (in src/backend/partitioning/partprune.c)

## Notes and Other Information
- Returns true if any Var node is found, false otherwise
- Only examines the current query level (varlevelsup = 0)
- Must only be used after sublinks have been reduced to subplans
- Does not examine subqueries - this is an important limitation
- Used extensively throughout PostgreSQL for variable dependency analysis
- Part of the optimizer's variable analysis utilities