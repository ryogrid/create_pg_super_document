# match_clause_to_partition_key

## Location
[src/backend/partitioning/partprune.c:1790-2437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partprune.c#L1790-L2437)

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
  - [match_boolean_partition_clause](match_boolean_partition_clause.md)
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)
  - [get_op_opfamily_properties](../g/get_op_opfamily_properties.md)
  - PartCollMatchesExprColl
  - [contain_var_clause](../c/contain_var_clause.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [pull_exec_paramids](../p/pull_exec_paramids.md)
- Called from:
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md)

## Notes and Other Information
The function returns different PartClauseMatchStatus values indicating the match result:
- PARTCLAUSE_MATCH_CLAUSE: Direct clause match, PartClauseInfo created
- PARTCLAUSE_MATCH_NULLNESS: NULL test match
- PARTCLAUSE_MATCH_STEPS: Complex clause requiring step generation
- PARTCLAUSE_MATCH_CONTRADICT: Self-contradictory clause
- PARTCLAUSE_NOMATCH: No match with this key, try others
- PARTCLAUSE_UNSUPPORTED: Clause form unsuitable for any partition key

Special handling exists for NOT IN operations with list partitioning and boolean partition keys with IS NOT TRUE/IS NOT FALSE tests.