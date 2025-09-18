# ExplainPrintPlan

## Location
src/backend/commands/explain.c: 877 - 941

## Overview
ExplainPrintPlan converts a QueryDesc's plan tree into textual representation and handles the core logic of generating EXPLAIN output for the execution plan structure.

## Definition


## Detailed Description
ExplainPrintPlan is responsible for the main task of converting an execution plan tree into human-readable or structured output. It performs several critical setup operations before generating the plan output:

1. **Plan Tree Setup**: Initializes ExplainState fields specific to the current plan tree, including the planned statement, range table, and deparse context for SQL reconstruction
2. **Relation Analysis**: Pre-scans the plan tree to determine which relations are actually used and creates appropriate names for display
3. **Special Gather Handling**: Implements special logic for "invisible" Gather nodes used in regression testing to ensure consistent output between parallel and non-parallel execution modes
4. **Plan Tree Traversal**: Recursively processes the entire plan tree starting from the top-level plan state
5. **Configuration Display**: Optionally includes modified GUC settings that affect query planning
6. **Query Identifier**: Shows the query identifier when verbose mode is enabled (except in regression testing mode)

The function coordinates the overall EXPLAIN output generation process, delegating specific formatting tasks to specialized functions while managing the global state needed for proper plan visualization.

## Parameters / Member Variables
- : ExplainState containing output formatting options, buffers, and state information for the current explain operation
- : QueryDesc containing the planned statement, execution state, and other query metadata needed for plan explanation

## Dependencies
- Functions called/Symbols referenced:
  - ExplainPreScanNode
  - select_rtable_names_for_explain
  - deparse_context_for_plan_tree
  - ExplainNode
  - ExplainPrintSettings
  - ExplainPropertyInteger
  - outerPlanState
- Called from (representative examples):
  - ExplainOnePlan

## Notes and Other Information
- The function will not work correctly on utility statements (only works with planned queries)
- Special handling exists for "invisible" Gather nodes to support regression testing with different debug_parallel_query settings
- The function assumes that ExplainState's basic fields (options, output buffer, formatting state) are already properly initialized
- Query identifiers are displayed as signed 64-bit integers to match pg_stat_statements output format
- Plan-tree-specific fields in ExplainState are initialized by this function and used by subsequent explain operations
- The deparse context created here enables proper SQL fragment reconstruction throughout the explanation process