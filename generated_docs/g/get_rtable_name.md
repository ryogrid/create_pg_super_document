# get_rtable_name

## Location
[src/backend/utils/adt/ruleutils.c:4946-4964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4946-L4964)

## Overview
A convenience function that retrieves the previously assigned alias name for a range table entry from the topmost namespace level in a deparse context.

## Definition
```c
static char *get_rtable_name(int rtindex, deparse_context *context)
```

## Detailed Description
This function provides a simple interface to access range table entry (RTE) alias names during SQL rule decompilation. It operates on the topmost namespace level within the provided deparse context, extracting the alias name for a given range table index. The function assumes that alias names have been previously assigned and stored in the rtable_names list within the deparse namespace.

The function performs bounds checking through an assertion to ensure the provided rtindex is valid (greater than 0 and within the bounds of the rtable_names list). It uses 1-based indexing for the rtindex parameter but converts to 0-based indexing when accessing the underlying list structure.

This is a foundational utility function used throughout the rule decompilation process whenever range table entry names need to be retrieved for generating SQL text output.

## Parameters / Member Variables
- `rtindex`: 1-based index of the range table entry whose name should be retrieved
- `context`: Pointer to the deparse_context containing namespace information and previously assigned RTE names

## Dependencies
- Functions called/Symbols referenced:
  - [deparse_context](../d/deparse_context.md) (context structure for rule decompilation)
  - deparse_namespace (namespace structure containing RTE information)
  - linitial (macro to get first element of a list)
  - [list_nth](../l/list_nth.md) (function to get nth element of a list)
  - [list_length](../l/list_length.md) (function to get length of a list)
- Called from (representative examples):
  - [get_select_query_def](get_select_query_def.md) (at line 5819)
  - [get_from_clause_item](get_from_clause_item.md) (at line 12309)
  - [get_rte_alias](get_rte_alias.md) (at line 12329)

## Notes and Other Information
- This is a static function, only accessible within ruleutils.c
- Uses 1-based indexing for rtindex parameter to match PostgreSQL's range table conventions
- Performs runtime assertion checking to validate the rtindex parameter
- The function assumes the RTE belongs to the topmost namespace level in the context
- Returns a direct pointer to the stored alias name string (not a copy)
- Essential for maintaining consistent table/relation naming during rule decompilation
- Part of the broader deparse context management system that tracks namespace information during SQL generation

## Simplified Source

```c
static char *
get_rtable_name(int rtindex, deparse_context *context)
{
    // Get the topmost namespace from context
    deparse_namespace *dpns = (deparse_namespace *) linitial(context->namespaces);

    // Validate rtindex is within bounds (1-based indexing)
    Assert(rtindex > 0 && rtindex <= list_length(dpns->rtable_names));

    // Return the RTE alias name (convert to 0-based index)
    return (char *) list_nth(dpns->rtable_names, rtindex - 1);
}
```