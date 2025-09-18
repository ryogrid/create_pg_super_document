# ExplainMemberNodes

## Location
src/backend/commands/explain.c: 4384 - 4401

## Overview
ExplainMemberNodes is a static function that explains the constituent child plans of composite plan nodes like Append, MergeAppend, BitmapAnd, or BitmapOr in PostgreSQL's EXPLAIN output.

## Definition
```c
static void ExplainMemberNodes(PlanState **planstates, int nplans, List *ancestors, ExplainState *es)
```

## Detailed Description
This function iterates through an array of child plan states and recursively calls ExplainNode for each one, labeling them as "Member" plans in the output. It's used by composite plan node types that contain multiple child plans, such as Append nodes (which union multiple relations), MergeAppend nodes (which merge-join multiple sorted inputs), and bitmap operation nodes (BitmapAnd/BitmapOr) that combine multiple bitmap index scans. The function maintains the ancestor chain for proper context in nested plan explanations.

## Parameters / Member Variables
- `planstates`: Array of pointers to PlanState structures representing the child plans to explain
- `nplans`: Number of child plans in the planstates array
- `ancestors`: List of ancestor plan nodes for maintaining context in nested explanations
- `es`: Pointer to ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - [ExplainNode](ExplainNode.md)
  - ExplainState (struct)
- Called from (representative examples):
  - [ExplainNode](ExplainNode.md) (for Append, MergeAppend, BitmapAnd, BitmapOr nodes)

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Used specifically for composite plan nodes that have multiple child plans
- Each child plan is labeled as a "Member" in the EXPLAIN output
- The ancestors list should already contain the immediate parent of these member plans when this function is called
- Part of PostgreSQL's hierarchical query execution plan explanation system
- Provides a uniform way to display child plans regardless of the specific composite node type