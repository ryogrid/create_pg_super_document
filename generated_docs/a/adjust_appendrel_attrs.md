# adjust_appendrel_attrs

## Location
src/backend/optimizer/util/appendinfo.c: 196 - 214

## Overview
Transforms query tree nodes by translating variable references from parent relations to their corresponding child relations using provided AppendRelInfo mappings.

## Definition


## Detailed Description
This function serves as the main entry point for translating parent relation references to child relation references in query trees. It sets up the necessary context and delegates the actual transformation work to adjust_appendrel_attrs_mutator. The function is essential for inheritance and partitioning scenarios where queries against parent tables need to be rewritten to operate on specific child tables. It handles not just Var nodes but also other elements like range table indexes that appear outside of variable references.

## Parameters / Member Variables
- : PlannerInfo containing planning context and information
- : The query tree node to be transformed (can be any Node type)
- : Number of AppendRelInfo structures in the array
- : Array of AppendRelInfo structures containing parent-to-child translation mappings

## Dependencies
- Functions called/Symbols referenced:
  - adjust_appendrel_attrs_mutator (performs the actual tree transformation)
  - adjust_appendrel_attrs_context (context structure for transformation)
- Called from (representative examples):
  - set_append_rel_size
  - add_child_rel_equivalences
  - try_partitionwise_join
  - apply_scanjoin_target_to_paths
  - adjust_appendrel_attrs_multilevel

## Notes and Other Information
- Only works on post-sublink-conversion trees, avoiding recursion into sub-queries
- Includes assertions to prevent misuse with Query trees and ensure valid input parameters
- Similar in concept to pullup_replace_vars() but specialized for inheritance/partitioning scenarios
- Acts as a wrapper that establishes context before calling the actual mutator function