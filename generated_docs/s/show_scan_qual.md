# show_scan_qual

## Location
src/backend/commands/explain.c: 2531 - 2544

## Overview
A specialized wrapper function for displaying qualifier expressions in scan plan nodes, automatically determining whether to use table prefixes based on the scan type and verbosity settings.

## Definition
```c
static void show_scan_qual(List *qual, const char *qlabel,
                          PlanState *planstate, List *ancestors,
                          ExplainState *es)
```

## Detailed Description
The `show_scan_qual` function is a specialized version of `show_qual` designed specifically for scan plan nodes in PostgreSQL's query execution plans. Its primary responsibility is to determine the appropriate prefix usage for displaying column references in qualification expressions based on the type of scan being performed and the verbosity level requested.

The function implements intelligent prefixing logic: it uses table prefixes when dealing with SubqueryScan nodes (where ambiguity is more likely) or when verbose output is requested. This helps users understand which table or subquery a column reference belongs to in complex queries. After determining the appropriate prefix setting, it delegates the actual formatting work to the more general `show_qual` function.

## Parameters / Member Variables
- `qual`: List of qualification expressions with implicit AND semantics to be displayed
- `qlabel`: The label to use when displaying this qualification in the EXPLAIN output
- `planstate`: The plan state containing execution context for the current scan node
- `ancestors`: List of ancestor plan nodes providing context for variable resolution
- `es`: The ExplainState structure containing output formatting options and verbosity settings

## Dependencies
- Functions called/Symbols referenced:
  - show_qual
  - SubqueryScan (type check via IsA macro)
- Called from (representative examples):
  - ExplainNode (multiple scan node types)

## Notes and Other Information
- This function serves as an intelligent adapter between scan-specific requirements and general qualification display
- The prefix decision logic handles two main cases: SubqueryScan complexity and user-requested verbosity
- Heavily used throughout ExplainNode for various scan types including SeqScan, IndexScan, BitmapScan, etc.
- Part of PostgreSQL's layered approach to EXPLAIN formatting, providing scan-specific intelligence while reusing general qualification logic
- The useprefix decision helps balance readability (shorter output) with clarity (unambiguous references)