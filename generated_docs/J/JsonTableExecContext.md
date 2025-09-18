# JsonTableExecContext

## Location
src/backend/utils/adt/jsonpath_exec.c: 223 - 235

## Overview
JsonTableExecContext is the top-level execution context structure that coordinates PostgreSQL's JSON_TABLE operation execution, managing both root plan state and per-column plan states.

## Definition
```c
typedef struct JsonTableExecContext
{
	int			magic;

	/* State of the plan providing a row evaluated from "root" jsonpath */
	JsonTablePlanState *rootplanstate;

	/*
	 * Per-column JsonTablePlanStates for all columns including the nested
	 * ones.
	 */
	JsonTablePlanState **colplanstates;
} JsonTableExecContext;
```

## Detailed Description
JsonTableExecContext serves as the master coordination structure for PostgreSQL's JSON_TABLE functionality. It acts as the central hub that manages the complete execution context of a JSON_TABLE operation. The structure maintains a reference to the root plan state that handles the primary jsonpath evaluation and an array of column-specific plan states that manage individual column processing. This design enables efficient coordination between the overall row generation process and the specific column value extraction operations. The magic number field likely serves as a validation mechanism to ensure the integrity of the execution context during operations.

## Parameters / Member Variables
- `magic`: Integer magic number used for context validation and integrity checking
- `rootplanstate`: Pointer to JsonTablePlanState that manages the root jsonpath evaluation and primary row generation
- `colplanstates`: Array of pointers to JsonTablePlanState structures, one for each column (including nested columns) in the JSON_TABLE

## Dependencies
- Functions called/Symbols referenced:
  - JsonTablePlanState (used for both root plan state and column plan states)
- Called from (representative examples):
  - JsonTableInitOpaque
  - JsonTableDestroyOpaque  
  - JsonTableSetDocument
  - JsonTableFetchRow
  - JsonTableGetValue
  - GetJsonTableExecContext

## Notes and Other Information
- This structure is part of PostgreSQL's JSON_TABLE execution framework
- Located in src/backend/utils/adt/jsonpath_exec.c at lines 223-235
- Serves as the top-level coordination point for all JSON_TABLE operations
- The colplanstates array handles both regular and nested columns uniformly
- The magic field suggests this structure may be used in contexts where type validation is important
- Acts as the bridge between PostgreSQL's table function infrastructure and JSON-specific processing logic
- Manages the lifecycle of both root-level and column-specific execution states