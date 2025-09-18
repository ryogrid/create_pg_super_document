# parse_sub_analyze

## Location
src/backend/parser/analyze.c: 221 - 247

## Overview
Entry point for recursively analyzing sub-statements within a larger query context, inheriting parse state from a parent statement.

## Definition


## Detailed Description
This function handles the analysis of sub-statements that occur within a larger SQL query context, such as subqueries, set operations, CTEs, and derived tables. Unlike the top-level parse_analyze functions, this function creates a child parse state that inherits context from a parent parse state, allowing for proper scope resolution and parameter inheritance.

The function performs several key setup operations:
1. Creates a child parse state linked to the parent
2. Sets up CTE context if analyzing within a Common Table Expression
3. Configures table locking inheritance from parent context
4. Sets unknown type resolution behavior
5. Transforms the statement using the recursive transformation logic
6. Cleans up the parse state

This function is essential for PostgreSQL's ability to handle complex nested query structures while maintaining proper scoping and context inheritance.

## Parameters / Member Variables
- : Node representing the sub-statement to be analyzed (not necessarily a RawStmt)
- : Parse state from the containing statement for context inheritance
- : Common Table Expression context if analyzing within a CTE
- : Whether table locking should be inherited from parent context
- : Whether to resolve unknown parameter types during analysis

## Dependencies
- Functions called/Symbols referenced:
  - [make_parsestate](../m/make_parsestate.md): Creates child parse state with parent context
  - [transformStmt](../t/transformStmt.md): Performs statement transformation for sub-statements
  - [free_parsestate](../f/free_parsestate.md): Cleanup parse state structure
  - CommonTableExpr: Structure representing Common Table Expression context

- Called from (representative examples):
  - [transformSetOperationTree](../t/transformSetOperationTree.md): For analyzing UNION/INTERSECT/EXCEPT operations
  - [transformRangeSubselect](../t/transformRangeSubselect.md): For analyzing subqueries in FROM clauses
  - [analyzeCTE](../a/analyzeCTE.md): For analyzing Common Table Expression definitions
  - transformSubLink: For analyzing subquery expressions

## Notes and Other Information
- This function handles recursive parsing for nested query structures
- Unlike top-level analyzers, it doesn't generate query IDs or invoke post-parse hooks
- The parent-child parse state relationship enables proper scope resolution
- Critical for handling subqueries, CTEs, set operations, and derived tables
- The locked_from_parent parameter ensures consistent locking behavior in nested contexts
- Does not perform the same level of post-processing as top-level parse functions
- Uses transformStmt rather than transformTopLevelStmt for recursive analysis