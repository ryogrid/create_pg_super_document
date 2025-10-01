# show_incremental_sort_group_info

## Location
[src/backend/commands/explain.c:3036-3149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L3036-L3149)

## Overview
Formats and displays incremental sort group statistics for EXPLAIN ANALYZE output, providing a comprehensive summary of sort methods, memory usage, and disk usage across all batches within an incremental sort group.

## Definition

```c
static void
show_incremental_sort_group_info(IncrementalSortGroupInfo *groupInfo,
								 const char *groupLabel, bool indent, ExplainState *es)
```
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
  - [tuplesort_method_name](../t/tuplesort_method_name.md): Gets human-readable name for sort methods
  - [tuplesort_space_type_name](../t/tuplesort_space_type_name.md): Gets human-readable name for space types
  - [appendStringInfoSpaces](../a/appendStringInfoSpaces.md): Adds indentation spaces to output buffer
  - [appendStringInfo](../a/appendStringInfo.md)/appendStringInfoString: Appends formatted text to output buffer
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

## Simplified Source

```c
static void
show_incremental_sort_group_info(IncrementalSortGroupInfo *groupInfo,
                                  const char *groupLabel, bool indent, ExplainState *es)
{
    List *methodNames = NIL;

    // Build list of sort methods used
    for (int bit = 0; bit < NUM_TUPLESORTMETHODS; bit++)
    {
        TuplesortMethod sortMethod = (1 << bit);
        if (groupInfo->sortMethods & sortMethod)
        {
            const char *methodName = tuplesort_method_name(sortMethod);
            methodNames = lappend(methodNames, unconstify(char *, methodName));
        }
    }

    if (es->format == EXPLAIN_FORMAT_TEXT)
    {
        // Text format output
        if (indent)
            appendStringInfoSpaces(es->str, es->indent * 2);

        appendStringInfo(es->str, "%s Groups: " INT64_FORMAT "  Sort Method",
                         groupLabel, groupInfo->groupCount);

        // Handle singular/plural for methods
        if (list_length(methodNames) > 1)
            appendStringInfoString(es->str, "s: ");
        else
            appendStringInfoString(es->str, ": ");

        // List sort methods
        ListCell *methodCell;
        foreach(methodCell, methodNames)
        {
            appendStringInfoString(es->str, (char *) methodCell->ptr_value);
            if (foreach_current_index(methodCell) < list_length(methodNames) - 1)
                appendStringInfoString(es->str, ", ");
        }

        // Show memory usage if applicable
        if (groupInfo->maxMemorySpaceUsed > 0)
        {
            int64 avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
            const char *spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_MEMORY);
            appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
                             spaceTypeName, avgSpace, spaceTypeName, groupInfo->maxMemorySpaceUsed);
        }

        // Show disk usage if applicable
        if (groupInfo->maxDiskSpaceUsed > 0)
        {
            int64 avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;
            const char *spaceTypeName = tuplesort_space_type_name(SORT_SPACE_TYPE_DISK);
            appendStringInfo(es->str, "  Average %s: " INT64_FORMAT "kB  Peak %s: " INT64_FORMAT "kB",
                             spaceTypeName, avgSpace, spaceTypeName, groupInfo->maxDiskSpaceUsed);
        }
    }
    else
    {
        // Structured format output (JSON/XML/YAML)
        StringInfoData groupName;
        initStringInfo(&groupName);
        appendStringInfo(&groupName, "%s Groups", groupLabel);

        ExplainOpenGroup("Incremental Sort Groups", groupName.data, true, es);
        ExplainPropertyInteger("Group Count", NULL, groupInfo->groupCount, es);
        ExplainPropertyList("Sort Methods Used", methodNames, es);

        // Memory space reporting
        if (groupInfo->maxMemorySpaceUsed > 0)
        {
            int64 avgSpace = groupInfo->totalMemorySpaceUsed / groupInfo->groupCount;
            // Create nested group for memory space details
            ExplainPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
            ExplainPropertyInteger("Peak Sort Space Used", "kB", groupInfo->maxMemorySpaceUsed, es);
        }

        // Disk space reporting
        if (groupInfo->maxDiskSpaceUsed > 0)
        {
            int64 avgSpace = groupInfo->totalDiskSpaceUsed / groupInfo->groupCount;
            // Create nested group for disk space details
            ExplainPropertyInteger("Average Sort Space Used", "kB", avgSpace, es);
            ExplainPropertyInteger("Peak Sort Space Used", "kB", groupInfo->maxDiskSpaceUsed, es);
        }

        ExplainCloseGroup("Incremental Sort Groups", groupName.data, true, es);
    }
}
```