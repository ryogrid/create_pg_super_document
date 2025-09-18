# flatCopyTargetEntry

## Location
src/backend/nodes/makefuncs.c: 320 - 333

## Overview
Creates a shallow copy of an existing TargetEntry node, duplicating the top-level structure without copying any substructure (child nodes).

## Definition
```c
TargetEntry *flatCopyTargetEntry(TargetEntry *src_tle)
```

## Detailed Description
The `flatCopyTargetEntry` function performs a shallow copy of a TargetEntry node using direct memory copy (memcpy). This is an efficient way to duplicate a TargetEntry when you need to modify certain fields like the result number (`resno`) or substitute a new expression without affecting the original node.

The function creates a new TargetEntry node and copies all field values from the source node. However, since this is a shallow copy, any pointer fields in the new node will point to the same substructures as the original node. This is commonly used in query optimization and rewriting phases where structural modifications are needed while preserving most of the original node's properties.

## Parameters / Member Variables
- `src_tle`: Source TargetEntry node to be copied (must be a valid TargetEntry node)

## Dependencies  
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation)
  - IsA (macro for node type checking)
  - Assert (assertion macro)
  - memcpy (C standard library memory copy function)
  - TargetEntry (node type being copied)
- Called from (representative examples):
  - rewriteTargetListIU (rewriter)
  - expand_insert_targetlist (optimizer prep)
  - set_upper_references (optimizer setrefs)
  - process_matched_tle (rewriter)

## Notes and Other Information
- This is a shallow copy operation - substructures like the expression (`expr`) field are not duplicated
- Commonly used when modifying TargetEntry properties without changing the underlying expression
- The Assert() ensures type safety by verifying the source is actually a TargetEntry node
- More efficient than deep copying when substructure sharing is acceptable or desired
- Located in src/backend/nodes/makefuncs.c:320-333