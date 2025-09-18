# contain_placeholder_references_walker

## Location
src/backend/optimizer/util/placeholder.c: 479 - 520

## Overview
A recursive walker function that traverses expression trees to detect PlaceHolderVar references to a specific relation ID, implementing the core logic for contain_placeholder_references_to.

## Definition
```c
static bool contain_placeholder_references_walker(Node *node,
                                                contain_placeholder_references_context *context)
```

## Detailed Description
This static function implements the recursive traversal logic for detecting placeholder references. It uses the standard PostgreSQL walker pattern to traverse expression trees, with special handling for PlaceHolderVars and Query nodes. When it encounters a PlaceHolderVar at the current query level, it checks if the target relation ID is present in the placeholder's phrels bitmap. The function avoids examining phnullingrels since it's looking for references in the contained expression, not outer joins that might null the result.

For Query nodes (subqueries), the function properly manages the sublevels_up counter to handle nested query levels. The function relies on the phrels field to adequately summarize what relations are referenced in the placeholder's contained expression, avoiding the need to recurse into the expression itself.

## Parameters / Member Variables
- `node`: Current node in the expression tree being examined
- `context`: Walker context containing the target relid and current sublevel information

## Dependencies
- Functions called/Symbols referenced:
  - [contain_placeholder_references_context](contain_placeholder_references_context.md) (context structure type)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (placeholder variable node type)
  - [bms_is_member](../b/bms_is_member.md) (checks if relid is member of bitmap set)
  - query_tree_walker (walker for Query nodes/subqueries)
  - expression_tree_walker (general expression tree walker)
  - [contain_placeholder_references_walker](contain_placeholder_references_walker.md) (recursive self-reference)
- Called from (representative examples):
  - [contain_placeholder_references_to](contain_placeholder_references_to.md) (src/backend/optimizer/util/placeholder.c:475)
  - [contain_placeholder_references_walker](contain_placeholder_references_walker.md) (recursive calls at lines 512, 518)

## Notes and Other Information
- This is a static function, only accessible within the placeholder.c file
- The function uses the standard PostgreSQL walker pattern for tree traversal
- For PlaceHolderVars, it only examines those at the current query level (phlevelsup == context->sublevels_up)
- The function explicitly avoids examining phnullingrels, focusing only on contained expression references
- It doesn't recurse into PlaceHolderVar expressions because phrels adequately summarizes the contained relations
- Proper sublevel management is implemented for handling nested subqueries
- The function returns true as soon as it finds a matching reference, providing early termination optimization