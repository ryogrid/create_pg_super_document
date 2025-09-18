# acquireLocksOnSubLinks

## Location
src/backend/rewrite/rewriteHandler.c: 308 - 348

## Overview
A walker function that finds sublink subqueries and recursively acquires rewrite locks on them as part of the AcquireRewriteLocks process.

## Definition
```c
static bool acquireLocksOnSubLinks(Node *node, acquireLocksOnSubLinks_context *context)
```

## Detailed Description
acquireLocksOnSubLinks is a specialized tree walker function designed to traverse expression trees and identify SubLink nodes (subqueries within expressions). When it encounters a SubLink, it recursively calls AcquireRewriteLocks on the subquery to ensure that all relations referenced within sublinks are properly locked.

The function is specifically designed to work with the expression_tree_walker framework and is called from AcquireRewriteLocks to handle subqueries that appear within expressions (as opposed to those in the FROM clause or CTEs, which are handled directly by AcquireRewriteLocks).

The function explicitly avoids recursing into Query nodes because AcquireRewriteLocks has already processed subselects of subselects, preventing duplicate processing and potential infinite recursion.

## Parameters / Member Variables
- `node`: The current node being examined in the expression tree traversal
- `context`: A context structure containing the for_execute flag that determines the locking behavior

## Dependencies
- Functions called/Symbols referenced:
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - expression_tree_walker
  - IsA (macro for type checking)
- Called from (representative examples):
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - [rewriteRuleAction](../r/rewriteRuleAction.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - [CopyAndAddInvertedQual](../C/CopyAndAddInvertedQual.md)
  - [rewriteTargetView](../r/rewriteTargetView.md)

## Notes and Other Information
- This is a static function, only accessible within the rewriteHandler.c file
- Uses the expression_tree_walker pattern for systematic tree traversal
- Specifically handles SubLink nodes while letting the walker handle other node types
- The forUpdatePushedDown parameter is always passed as false when calling AcquireRewriteLocks from this function
- Part of PostgreSQL's comprehensive locking strategy to ensure schema stability during query rewriting and execution