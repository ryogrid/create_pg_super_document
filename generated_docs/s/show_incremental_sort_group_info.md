# show_incremental_sort_group_info

## Location
[src/backend/commands/explain.c:3036-3149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3036-L3149)

## Overview
Formats and displays incremental sort group statistics for EXPLAIN ANALYZE output, providing a comprehensive summary of sort methods, memory usage, and disk usage across all batches within an incremental sort group.

## Definition


## Detailed Description
This function is a critical component of PostgreSQL's EXPLAIN ANALYZE functionality for incremental sort nodes. Incremental sort operations process data in potentially very large numbers of batches, and this function aggregates the tuplesort statistics from each batch into an intelligible summary for display.

The function handles both text and structured (JSON/XML/YAML) output formats. For text format, it creates a human-readable summary showing group counts, sort methods used, and memory/disk space statistics. For structured formats, it uses the ExplainOpenGroup/ExplainCloseGroup framework to create properly nested output.

Key features include:
- Aggregation of sort methods used across all batches in a group
- Calculation of average and peak memory/disk usage statistics  
- Support for multiple output formats (text vs structured)
- Proper pluralization of method names in text output
- Memory and disk space reporting in kilobytes

## Parameters / Member Variables
- : Pointer to IncrementalSortGroupInfo structure containing aggregated statistics for the sort group
- : String label identifying the type of group (e.g., "Full-sort", "Pre-sorted")
- : Boolean flag indicating whether to indent the output (used for text format alignment)
- : Pointer to ExplainState structure containing output formatting context and buffers

## Dependencies
- Functions called/Symbols referenced:
  - tuplesort_method_name: Gets human-readable name for sort methods
  - tuplesort_space_type_name: Gets human-readable name for space types
  - appendStringInfoSpaces: Adds indentation spaces to output buffer
  - appendStringInfo/appendStringInfoString: Appends formatted text to output buffer
  - [ExplainOpenGroup](../E/ExplainOpenGroup.md)/ExplainCloseGroup: Creates structured output groups
  - [ExplainPropertyInteger](../E/ExplainPropertyInteger.md)/ExplainPropertyList: Adds properties to structured output
  - unconstify: Removes const qualifier for list operations
  - foreach/foreach_current_index: List iteration macros
- Called from (representative examples):
  - [show_incremental_sort_info](show_incremental_sort_info.md): Main incremental sort display function

## Notes and Other Information
- This is a static function used internally within explain.c for incremental sort reporting
- The function handles the complexity of multiple sort methods being used within a single group
- Memory and disk space are reported in kilobytes for consistency with other PostgreSQL memory reporting
- The function properly handles cases where no disk space was used (disk-based sorting not required)
- Text format output includes careful formatting for readability, including proper comma separation for multiple sort methods
- The structured format creates nested groups for different types of space usage statistics