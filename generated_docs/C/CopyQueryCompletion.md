# CopyQueryCompletion

## Location
[src/include/tcop/cmdtag.h:45-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/cmdtag.h#L45-L62)

## Overview
A static inline function that copies the contents of one QueryCompletion structure to another, transferring both the command tag and the number of processed rows.

## Definition
```c
static inline void CopyQueryCompletion(QueryCompletion *dst, const QueryCompletion *src)
```

## Detailed Description
CopyQueryCompletion performs a simple member-wise copy from a source QueryCompletion structure to a destination QueryCompletion structure. This function is used when query completion information needs to be transferred or preserved across different execution contexts, such as when moving results from one portal to another or when aggregating completion information from multiple operations.

The function is defined as a static inline function in the header file, allowing for efficient inline expansion during compilation.

## Parameters / Member Variables
- `dst`: Pointer to the destination QueryCompletion structure that will receive the copied values
- `src`: Pointer to the source QueryCompletion structure (marked const to indicate it will not be modified)

## Dependencies
- Functions called/Symbols referenced:
  - [QueryCompletion](../Q/QueryCompletion.md) (structure type)
  - [InitializeQueryCompletion](../I/InitializeQueryCompletion.md)
  - [GetCommandTagName](../G/GetCommandTagName.md)
  - CommandTag (enum type)
  - [GetCommandTagNameAndLen](../G/GetCommandTagNameAndLen.md)
  - [command_tag_display_rowcount](../c/command_tag_display_rowcount.md)
  - [command_tag_event_trigger_ok](../c/command_tag_event_trigger_ok.md)
  - [command_tag_table_rewrite_ok](../c/command_tag_table_rewrite_ok.md)
  - [GetCommandTagEnum](../G/GetCommandTagEnum.md)
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md)
- Called from (representative examples):
  - [PortalRun](../P/PortalRun.md)
  - [FillPortalStore](../F/FillPortalStore.md)
  - [PortalRunMulti](../P/PortalRunMulti.md)

## Notes and Other Information
- This is a static inline function defined in src/include/tcop/cmdtag.h
- The function performs a shallow copy of the QueryCompletion structure members
- Used primarily in portal execution contexts where completion information needs to be transferred between different execution phases
- The const qualifier on the src parameter ensures the source structure is not inadvertently modified
- This function is particularly useful in multi-statement execution scenarios where individual command results need to be preserved and combined

## Simplified Source

```c
// Simplified version of CopyQueryCompletion
static inline void CopyQueryCompletion(QueryCompletion *dst, const QueryCompletion *src) {
    // Simple member-wise copy of completion data
    dst->commandTag = src->commandTag;
    dst->nprocessed = src->nprocessed;
}
```

Key simplifications made:
- Added clarifying comment about the function's purpose
- This function is already very simple and straightforward
- Preserved the inline optimization and const correctness
- The core logic remains: copy command tag and processed row count