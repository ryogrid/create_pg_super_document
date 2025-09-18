# expression_planner

## Location
src/backend/optimizer/plan/planner.c: 6658 - 6684

## Overview
Performs planner transformations on standalone expressions that are not part of a plannable query, preparing them for execution by the PostgreSQL executor.

## Definition


## Detailed Description
The  function transforms parser output expressions into executable form for utility commands that need to evaluate expressions outside of regular query planning. The function performs two main transformations:

1. **Expression evaluation**: Calls  to convert named-argument function calls to positional notation, insert function default arguments, and simplify constant subexpressions
2. **Operator function ID resolution**: Uses  to fill in missing operator function IDs

The function explicitly disallows sublinks in standalone expressions, so no real "planning" occurs. It's designed for expressions that need immediate evaluation rather than complex query planning. The function creates a new expression tree without modifying the original input.

## Parameters / Member Variables
- : Input expression tree from the parser that needs to be transformed for execution

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs constant folding and function call normalization
  -  - Resolves missing operator function IDs
- Called from (representative examples):
  -  - For storing attribute defaults
  -  - During COPY command processing
  -  - When adding table columns
  -  - For preparing expressions in executor
  -  - During partition bound processing

## Notes and Other Information
- Returns results in current memory context, which can lead to memory leaks if not managed properly
- Does not track expression dependencies, making results suitable only for current query duration
- For longer-term caching needs, use  instead
- Creates a completely new expression tree rather than modifying the input
- Primarily used by utility commands rather than regular query execution
- The constant simplification side-effect is beneficial for expressions evaluated multiple times