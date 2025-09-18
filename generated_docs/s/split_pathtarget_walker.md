# split_pathtarget_walker

## Location
src/backend/optimizer/util/tlist.c: 1077 - 1201

## Overview
A recursive tree walker function that examines expressions to identify and categorize set-returning functions (SRFs) and variables for the split_pathtarget_at_srfs operation.

## Definition
static bool split_pathtarget_walker(Node *node, split_pathtarget_context *context)

## Detailed Description
This static helper function performs a recursive traversal of expression trees to analyze and categorize different types of nodes for SRF splitting. It operates as a callback function for expression_tree_walker and maintains context about the current traversal state.

The function handles several distinct cases:
1. Expressions already present in input_target are treated as variables (since setrefs.c will convert them to Vars)
2. Variable-like constructs (Var, PlaceHolderVar, Aggref, GroupingFunc, WindowFunc) are recorded as input variables
3. Set-returning function calls are recursively analyzed to determine their nesting depth and recorded at appropriate levels
4. Other scalar expressions are recursively traversed to examine their inputs

The function maintains and updates the context structure with information about required input variables, input SRFs, and the current depth of SRF nesting. It ensures that SRFs are properly categorized by their nesting level so that split_pathtarget_at_srfs can create appropriate evaluation levels.

## Parameters / Member Variables
- node: The current node being examined in the expression tree
- context: The split_pathtarget_context structure containing traversal state and output lists

## Dependencies
- Functions called/Symbols referenced:
  - expression_tree_walker (recursively traverses expression trees)
  - [list_member](../l/list_member.md) (checks if expression already exists in input_target)
  - IS_SRF_CALL (macro to identify set-returning function calls)
  - Various list manipulation functions (lappend, list_concat, list_nth_cell)
  - [Node](../N/Node.md) type checking macros (IsA for Var, PlaceHolderVar, Aggref, etc.)
- Called from (representative examples):
  - [split_pathtarget_at_srfs](split_pathtarget_at_srfs.md) (in src/backend/optimizer/util/tlist.c:943)
  - Recursively calls itself (in src/backend/optimizer/util/tlist.c:1142, 1184)

## Notes and Other Information
- This is a static function internal to tlist.c, designed specifically for SRF analysis
- Makes no effort to prevent duplicate entries in output lists (duplicates are handled elsewhere)
- Uses expression_tree_walker pattern for systematic tree traversal
- Handles sortgroupref preservation for expressions that appear in input_target
- Maintains proper depth tracking for nested SRFs to ensure correct level assignment
- Resets current_sgref to 0 for subexpressions since they are not sortgroup items
- Critical for the proper functioning of split_pathtarget_at_srfs in creating multi-level PathTarget hierarchies