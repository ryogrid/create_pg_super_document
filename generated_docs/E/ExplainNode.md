# ExplainNode

## Location
src/backend/commands/explain.c: 1367 - 2428

## Overview
The main function that generates detailed explanation output for a single plan node in PostgreSQL's EXPLAIN command.

## Definition
```c
static void ExplainNode(PlanState *planstate, List *ancestors, const char *relationship, const char *plan_name, ExplainState *es)
```

## Detailed Description
The `ExplainNode` function is the core function responsible for generating detailed textual or structured output for individual plan nodes in PostgreSQL's EXPLAIN functionality. It handles all major plan node types including scans, joins, aggregates, sorts, and many others. The function performs several key operations:

1. **Node Type Identification**: Uses a large switch statement to identify the specific plan node type and set appropriate display names for both text and structured output formats.

2. **Instrumentation Data Processing**: Extracts and formats execution statistics when ANALYZE option is used, including timing information, row counts, and loop counts.

3. **Worker State Management**: Handles parallel query execution details, including per-worker statistics when available.

4. **Format-specific Output**: Generates different output formats (text, JSON, XML, YAML) based on the ExplainState configuration.

5. **Node-specific Details**: Calls specialized functions to display details specific to each node type, such as index conditions, join conditions, sort keys, etc.

The function is highly recursive through its interaction with other explain functions and handles the complete tree traversal for plan explanation.

## Parameters / Member Variables
- `planstate`: PlanState node containing both plan structure and execution instrumentation data
- `ancestors`: List of parent Plan and SubPlan nodes for parameter interpretation context
- `relationship`: String describing relationship to parent node (e.g., "Outer", "Inner"), can be NULL at top level
- `plan_name`: Optional name to attach to the node, typically for subplans
- `es`: ExplainState structure containing output format, verbosity options, and output buffer

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag
  - [ExplainCreateWorkersState](ExplainCreateWorkersState.md)
  - [ExplainOpenGroup](ExplainOpenGroup.md)/ExplainCloseGroup
  - [ExplainPropertyText](ExplainPropertyText.md)/ExplainPropertyBool/ExplainPropertyFloat/ExplainPropertyInteger
  - [ExplainIndentText](ExplainIndentText.md)
  - [InstrEndLoop](../I/InstrEndLoop.md)
  - [show_plan_tlist](../s/show_plan_tlist.md)
  - [show_scan_qual](../s/show_scan_qual.md)
  - [show_upper_qual](../s/show_upper_qual.md)
  - Various node-specific show functions
- Called from (representative examples):
  - [ExplainPrintPlan](ExplainPrintPlan.md)
  - [ExplainSubPlans](ExplainSubPlans.md)

## Notes and Other Information
- Function exceeds 1000 lines due to comprehensive handling of all PostgreSQL plan node types
- Supports both text and structured output formats with different formatting logic
- Handles instrumentation cleanup through InstrEndLoop calls
- Manages indentation for text format output to create readable nested structure
- Includes detailed per-worker execution statistics for parallel queries
- Handles cost estimation display when costs option is enabled
- Contains extensive node-type-specific logic for displaying relevant execution details
- Critical function for PostgreSQL query analysis and performance debugging