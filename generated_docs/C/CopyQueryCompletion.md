# CopyQueryCompletion

## Location
src/include/tcop/cmdtag.h: 45 - 62

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
  - QueryCompletion (structure type)
  - InitializeQueryCompletion
  - GetCommandTagName
  - CommandTag (enum type)
  - GetCommandTagNameAndLen
  - command_tag_display_rowcount
  - command_tag_event_trigger_ok
  - command_tag_table_rewrite_ok
  - GetCommandTagEnum
  - BuildQueryCompletionString
- Called from (representative examples):
  - PortalRun
  - FillPortalStore
  - PortalRunMulti

## Notes and Other Information
- This is a static inline function defined in src/include/tcop/cmdtag.h
- The function performs a shallow copy of the QueryCompletion structure members
- Used primarily in portal execution contexts where completion information needs to be transferred between different execution phases
- The const qualifier on the src parameter ensures the source structure is not inadvertently modified
- This function is particularly useful in multi-statement execution scenarios where individual command results need to be preserved and combined