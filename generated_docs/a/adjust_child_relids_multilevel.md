# adjust_child_relids_multilevel

## Location
src/backend/optimizer/util/appendinfo.c: 588 - 627

## Overview
Substitutes child relation IDs for parent relation IDs in a Relids set, handling multi-level inheritance hierarchies where the child relation can be multiple inheritance levels below the parent.

## Definition
```c
Relids adjust_child_relids_multilevel(PlannerInfo *root, Relids relids, RelOptInfo *childrel, RelOptInfo *parentrel)
```

## Detailed Description
This function extends the basic relation ID substitution functionality to handle complex inheritance hierarchies where a child relation may be separated from the target parent by multiple intermediate inheritance levels. It implements a recursive approach to traverse the inheritance chain from child to parent, applying the necessary transformations at each level.

The function first performs an optimization check to see if the input relids set overlaps with the parent's relation IDs - if there's no overlap, no transformation is needed. For multi-level cases, it recursively calls itself to handle intermediate parent levels before applying the final transformation using adjust_child_relids.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planning context and information
- `relids`: Input Relids set containing relation IDs to be processed  
- `childrel`: RelOptInfo structure representing the child relation
- `parentrel`: RelOptInfo structure representing the target parent relation

## Dependencies
- Functions called/Symbols referenced:
  - AppendRelInfo (structure type)
  - bms_overlap (checks if two bitmap sets have common elements)
  - adjust_child_relids_multilevel (recursive self-call)
  - find_appinfos_by_relids (finds AppendRelInfo structures for given relation IDs)
  - adjust_child_relids (performs actual relation ID substitution)
- Called from (representative examples):
  - adjust_child_relids_multilevel (recursive calls)
  - REPARAMETERIZE_CHILD_PATH_LIST

## Notes and Other Information
- Implements recursive traversal of inheritance hierarchies to handle arbitrarily deep nesting
- Includes error checking to ensure the child relation is actually related to the specified parent
- Uses optimization to avoid unnecessary work when no relevant relation IDs are present
- Essential for PostgreSQL's handling of complex partitioned table hierarchies and inheritance chains
- Memory management includes proper cleanup of allocated AppendRelInfo arrays