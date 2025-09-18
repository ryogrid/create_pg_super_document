# match_clause_to_partition_key

## Location
src/backend/partitioning/partprune.c: 1790 - 2437

## Overview
Attempts to match a given clause with a specified partition key and determines how the clause can be used for partition pruning.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's partition pruning mechanism. It analyzes various types of SQL clauses (WHERE conditions, JOIN conditions, etc.) to determine if they can be used to eliminate irrelevant partitions during query execution. The function supports multiple clause types including:

- Boolean partition clauses (for boolean partition keys)
- Binary operator expressions (=, <, >, <=, >=, <>)
- Scalar array operations (IN, NOT IN, ANY, ALL)
- NULL test expressions (IS NULL, IS NOT NULL)

The function performs extensive validation including operator family membership checks, collation matching, mutability analysis, and parameter detection to ensure the clause is suitable for pruning at the target execution phase (planner vs executor).

## Parameters / Member Variables
- : Context information for generating pruning steps, including target phase and partition relation info
- : The SQL expression/clause to be matched against the partition key
- : The partition key expression to match against
- : Index of the partition key in the partition scheme
- : Output parameter set when clause matches NULL/NOT NULL tests
- : Output parameter set to PartClauseInfo when clause can be directly used for pruning
- : Output parameter set to list of generated pruning steps for complex clauses

## Dependencies
- Functions called/Symbols referenced:
  - match_boolean_partition_clause
  - gen_partprune_steps_internal
  - get_op_opfamily_properties
  - PartCollMatchesExprColl
  - contain_var_clause
  - contain_volatile_functions
  - pull_exec_paramids
- Called from:
  - gen_partprune_steps_internal

## Notes and Other Information
The function returns different PartClauseMatchStatus values indicating the match result:
- PARTCLAUSE_MATCH_CLAUSE: Direct clause match, PartClauseInfo created
- PARTCLAUSE_MATCH_NULLNESS: NULL test match
- PARTCLAUSE_MATCH_STEPS: Complex clause requiring step generation
- PARTCLAUSE_MATCH_CONTRADICT: Self-contradictory clause
- PARTCLAUSE_NOMATCH: No match with this key, try others
- PARTCLAUSE_UNSUPPORTED: Clause form unsuitable for any partition key

Special handling exists for NOT IN operations with list partitioning and boolean partition keys with IS NOT TRUE/IS NOT FALSE tests.