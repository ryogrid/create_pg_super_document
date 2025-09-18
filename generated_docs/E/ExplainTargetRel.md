# ExplainTargetRel

## Location
src/backend/commands/explain.c: 4034 - 4171

## Overview
ExplainTargetRel is a static function that displays the target relation information for various types of scan and modify operations in PostgreSQL's EXPLAIN output.

## Definition
```c
static void ExplainTargetRel(Plan *plan, Index rti, ExplainState *es)
```

## Detailed Description
This function is responsible for showing the target relation of scan or modify nodes in EXPLAIN output. It handles a wide variety of plan node types including sequential scans, index scans, function scans, table function scans, CTE scans, and modify operations. The function extracts the appropriate object name (relation name, function name, CTE name, etc.) based on the plan node type and formats it according to the EXPLAIN output format (text or structured). It also handles namespace information and alias names when verbose mode is enabled.

## Parameters / Member Variables
- `plan`: Pointer to the Plan node for which to show the target relation
- `rti`: Range Table Index identifying which range table entry to use  
- `es`: Pointer to the ExplainState structure controlling output format and options

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - list_nth  
  - nodeTag
  - get_rel_name
  - get_namespace_name_or_temp
  - get_rel_namespace
  - get_func_name
  - get_func_namespace
  - quote_identifier
  - ExplainPropertyText
- Called from (representative examples):
  - ExplainScanTarget
  - ExplainModifyTarget
  - show_modifytable_info

## Notes and Other Information
- This is a static function, only accessible within the explain.c file
- Handles numerous plan node types: SeqScan, IndexScan, FunctionScan, TableFuncScan, ValuesScan, CteScan, ModifyTable, etc.
- Supports both text and structured (JSON/XML/YAML) output formats
- In verbose mode, includes schema/namespace information
- For function scans, attempts to extract the actual function name when possible
- Handles special cases like CTE self-references (WorkTableScan vs CteScan)
- Part of PostgreSQL's comprehensive query execution plan explanation system