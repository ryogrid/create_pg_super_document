# contain_var_clause_walker

## Location
[src/backend/optimizer/util/var.c:409-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L409-L440)

## Overview
A tree walker callback function that searches for Var, CurrentOfExpr, or PlaceHolderVar nodes at the current query level and returns true if any are found.

## Definition

```c
static bool
contain_var_clause_walker(Node *node, void *context)
```
## Detailed Description
The  function implements the core logic for detecting variable references at the current query level (level 0). It serves as a callback function for expression tree walking and is designed for early termination - as soon as any variable reference is found, it returns true to abort further tree traversal.

The function checks for three types of nodes that are considered "variables":
- **Var nodes**: Standard column references, checked for 
- **CurrentOfExpr nodes**: CURRENT OF cursor expressions, always considered variables
- **PlaceHolderVar nodes**: Placeholder variables, checked for 

For PlaceHolderVar nodes, if the placeholder is not at the current level, the function continues to examine the contained expression rather than stopping traversal.

This function is optimized for boolean queries - it only needs to determine presence/absence of variables, not collect them, so it can terminate early upon finding the first match.

## Parameters / Member Variables
- : The current node being examined during tree traversal
- : Context parameter (unused in this function, passed through to expression_tree_walker)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking macros)
  - expression_tree_walker (recursive expression tree traversal)
  - [Var](../V/Var.md), CurrentOfExpr, PlaceHolderVar (node type structures)
- Called from (representative examples):
  - [contain_var_clause](contain_var_clause.md) (primary caller)
  - [contain_var_clause_walker](contain_var_clause_walker.md) (recursive self-calls via expression_tree_walker)

## Notes and Other Information
- This is a static function, internal to var.c
- Implements early termination optimization - returns true immediately upon finding a variable
- Only examines the current query level (varlevelsup/phlevelsup == 0)
- [CurrentOfExpr](../C/CurrentOfExpr.md) nodes are always treated as variables regardless of level
- For PlaceHolderVar nodes, continues examining contained expressions if not at current level
- Does not examine subqueries - must only be used after sublink reduction
- Returns true to halt traversal (variable found), false to continue traversal

## Simplified Source

```c
static bool contain_var_clause_walker(Node *node, void *context) {
    if (node == NULL)
        return false;

    // Check for Var nodes at current level
    if (IsA(node, Var)) {
        if (((Var *) node)->varlevelsup == 0)
            return true;  // Found variable at current level
        return false;
    }

    // CurrentOfExpr is always considered a variable
    if (IsA(node, CurrentOfExpr))
        return true;

    // Check PlaceHolderVar nodes at current level
    if (IsA(node, PlaceHolderVar)) {
        if (((PlaceHolderVar *) node)->phlevelsup == 0)
            return true;  // Found placeholder at current level
        // Continue checking contained expression
    }

    // Recursively examine child nodes
    return expression_tree_walker(node, contain_var_clause_walker, context);
}
```