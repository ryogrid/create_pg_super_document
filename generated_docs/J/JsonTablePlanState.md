# JsonTablePlanState

## Location
[src/backend/utils/adt/jsonpath_exec.c:179-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L179-L218)

## Overview
JsonTablePlanState represents the execution state for evaluating row patterns derived from jsonpath expressions in PostgreSQL's JSON_TABLE functionality, managing the complete lifecycle of JSON data processing.

## Definition
```c
typedef struct JsonTablePlanState
{
	/* Original plan */
	JsonTablePlan *plan;

	/* The following fields are only valid for JsonTablePathScan plans */

	/* jsonpath to evaluate against the input doc to get the row pattern */
	JsonPath   *path;

	/*
	 * Memory context to use when evaluating the row pattern from the jsonpath
	 */
	MemoryContext mcxt;

	/* PASSING arguments passed to jsonpath executor */
	List	   *args;

	/* List and iterator of jsonpath result values */
	JsonValueList found;
	JsonValueListIterator iter;

	/* Currently selected row for JsonTableGetValue() to use */
	JsonTablePlanRowSource current;

	/* Counter for ORDINAL columns */
	int			ordinal;

	/* Nested plan, if any */
	struct JsonTablePlanState *nested;

	/* Left sibling, if any */
	struct JsonTablePlanState *left;

	/* Right sibling, if any */
	struct JsonTablePlanState *right;

	/* Parent plan, if this is a nested plan */
	struct JsonTablePlanState *parent;
} JsonTablePlanState;
```

## Detailed Description
JsonTablePlanState is the central execution state structure for PostgreSQL's JSON_TABLE functionality. It manages the complex process of evaluating jsonpath expressions against input documents to produce row patterns. The structure handles both simple path scans and complex nested execution plans with hierarchical relationships between parent and child plans. It maintains iteration state over jsonpath results, tracks ordinal counters for positional columns, and manages memory contexts for efficient resource usage during JSON processing operations.

## Parameters / Member Variables
- `plan`: Pointer to the original JsonTablePlan that defines the execution strategy
- `path`: JsonPath expression to evaluate against input documents for row pattern generation (valid only for JsonTablePathScan plans)
- `mcxt`: Memory context used for jsonpath evaluation to ensure proper memory management
- `args`: List of PASSING arguments provided to the jsonpath executor
- `found`: JsonValueList containing the results from jsonpath evaluation
- `iter`: JsonValueListIterator for traversing through the found results
- `current`: JsonTablePlanRowSource representing the currently selected row for column value computation
- `ordinal`: Integer counter used for ORDINAL column generation
- `nested`: Pointer to nested child plan state for hierarchical JSON_TABLE operations
- `left`: Pointer to left sibling plan state in the execution tree
- `right`: Pointer to right sibling plan state in the execution tree
- `parent`: Pointer to parent plan state when this is a nested plan

## Dependencies
- Functions called/Symbols referenced:
  - JsonTablePlan
  - JsonPath
  - [JsonValueList](JsonValueList.md)
  - [JsonValueListIterator](JsonValueListIterator.md)
  - [JsonTablePlanRowSource](JsonTablePlanRowSource.md)
- Called from (representative examples):
  - [JsonTableExecContext](JsonTableExecContext.md) (contains this as a member)
  - [JsonTableInitPlan](JsonTableInitPlan.md)
  - [JsonTableResetRowPattern](JsonTableResetRowPattern.md)
  - [JsonTablePlanNextRow](JsonTablePlanNextRow.md)
  - [JsonTableGetValue](JsonTableGetValue.md)

## Notes and Other Information
- This structure is part of PostgreSQL's JSON_TABLE execution framework
- Located in src/backend/utils/adt/jsonpath_exec.c at lines 179-218
- Supports complex hierarchical execution with nested plans and sibling relationships
- The structure is designed to handle both simple path scans and complex join operations
- Self-referential pointers enable tree-like execution plan structures
- Memory context management ensures efficient resource usage during JSON processing
- The ordinal counter supports PostgreSQL's ORDINAL column feature in JSON_TABLE