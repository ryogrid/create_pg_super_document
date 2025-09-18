# adjust_child_relids

## Location
src/backend/optimizer/util/appendinfo.c: 554 - 587

## Overview
Substitutes child relation IDs for parent relation IDs in a Relids set based on the provided AppendRelInfo mappings.

## Definition


## Detailed Description
This function performs relation ID substitution for query planning optimization, specifically for handling inheritance and partitioning scenarios. It iterates through an array of AppendRelInfo structures and replaces any parent relation IDs found in the input Relids set with their corresponding child relation IDs. The function is designed to be efficient by only creating a copy of the input set when modifications are actually needed.

The substitution process removes the parent relation ID from the set and adds the corresponding child relation ID. This is essential for query optimization when dealing with partitioned tables or inheritance hierarchies where the planner needs to work with specific child relations rather than abstract parent relations.

## Parameters / Member Variables
- : Input Relids set containing relation IDs to be processed
- : Number of AppendRelInfo structures in the appinfos array
- : Array of AppendRelInfo pointers containing parent-to-child relation ID mappings

## Dependencies
- Functions called/Symbols referenced:
  - AppendRelInfo (structure type)
  - bms_is_member (checks if relation ID exists in set)
  - bms_copy (creates copy of bitmap set)
  - bms_del_member (removes relation ID from set)
  - bms_add_member (adds relation ID to set)
- Called from (representative examples):
  - try_partitionwise_join
  - build_child_join_sjinfo
  - adjust_appendrel_attrs_mutator
  - adjust_child_relids_multilevel
  - build_child_join_rel

## Notes and Other Information
- Returns the original input set unchanged if no substitutions are needed, avoiding unnecessary memory allocation
- Only creates a modified copy when actual changes are required, optimizing memory usage
- Part of PostgreSQL's append relation handling infrastructure for inheritance and partitioning
- Used extensively in join planning and relation processing for partitioned tables