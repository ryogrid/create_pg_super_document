# fireRIRonSubLink

## Location
[src/backend/rewrite/rewriteHandler.c:1945-1981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1945-L1981)

## Overview
Applies fireRIRrules() to each SubLink (subselect in expression) found in the given parse tree, rewriting subqueries in-place and tracking row security information.

## Definition

```c
static bool
fireRIRonSubLink(Node *node, fireRIRonSubLink_context *context)
```
## Detailed Description
fireRIRonSubLink is a tree walker function that processes SubLink nodes (subselects within expressions) by applying rule rewriting to their subqueries. Unlike typical tree walkers, this function modifies SubLink nodes in-place, replacing the SubLink's subselect with a potentially rewritten version. The function also tracks whether any of the processed sublinks have row security enabled, aggregating this information in the context structure.

The function operates as part of the rule rewriting system (RIR - Rules Instead Rewrite) and ensures that subqueries within expressions are properly processed for rule application. It takes special care not to recurse into Query nodes since fireRIRrules already handles nested subselects.

## Parameters / Member Variables
- : The parse tree node to examine for SubLink nodes
- : Context structure containing:
  - : List of currently active rule rewrite information
  - : Boolean flag tracking if any sublink has row security

## Dependencies
- Functions called/Symbols referenced:
  - [fireRIRrules](fireRIRrules.md)
  - expression_tree_walker
  - IsA (macro)
  - SubLink (struct)
  - [fireRIRonSubLink_context](fireRIRonSubLink_context.md) (struct)
- Called from (representative examples):
  - [fireRIRrules](fireRIRrules.md) (multiple locations in rewriteHandler.c)

## Notes and Other Information
- Modifies SubLink nodes in-place, requiring caller responsibility for side-effects
- Uses expression_tree_walker for tree traversal but avoids recursing into Query nodes
- Part of the PostgreSQL rule rewriting system for handling views and rules
- Specifically designed to handle subselects within expressions rather than top-level queries
- Tracks row security information across all processed sublinks