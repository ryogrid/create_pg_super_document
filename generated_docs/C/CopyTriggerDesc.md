# CopyTriggerDesc

## Location
[src/backend/commands/trigger.c:2085-2139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2085-L2139)

## Overview
CopyTriggerDesc creates a deep copy of a TriggerDesc data structure, allocating the copy in the current memory context and duplicating all variable-length fields.

## Definition
TriggerDesc *CopyTriggerDesc(TriggerDesc *trigdesc)

## Detailed Description
This function performs a complete deep copy of a TriggerDesc structure, which is essential for proper memory management in PostgreSQL's trigger system. The function:

1. **Null/Empty Check**: Returns NULL immediately if the input TriggerDesc is NULL or contains no triggers.

2. **Structure Copying**: Creates a new TriggerDesc structure and copies the basic fields using memcpy, then allocates and copies the trigger array.

3. **Deep Copying of Variable-Length Fields**: For each trigger in the array, performs deep copying of:
   - **tgname**: Trigger name string
   - **tgattr**: Array of column attribute numbers (for column-specific triggers)
   - **tgargs**: Array of trigger function argument strings
   - **tgqual**: WHEN clause qualification expression string
   - **tgoldtable**: OLD TABLE transition table name
   - **tgnewtable**: NEW TABLE transition table name

4. **Memory Allocation**: Uses palloc() to allocate memory in the current memory context, making the copy suitable for the context where it's called.

5. **String Duplication**: Uses pstrdup() for all string fields to ensure proper memory management and avoid dangling pointer issues.

The function is crucial for scenarios where trigger descriptors need to exist in different memory contexts, such as when copying from working memory to cache memory context.

## Parameters / Member Variables
- : Pointer to source TriggerDesc structure to copy (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md): Memory allocation in current context
  - memcpy: Memory copying for structures and arrays
  - [pstrdup](../p/pstrdup.md): String duplication with proper memory management

- Called from (representative examples):
  - [RelationBuildTriggers](../R/RelationBuildTriggers.md): When copying trigger descriptor to cache memory
  - [InitResultRelInfo](../I/InitResultRelInfo.md): When initializing result relation information

## Notes and Other Information
- Returns NULL if input is NULL or contains no triggers
- Allocates memory in the current memory context, making it suitable for different memory management scenarios
- Performs deep copying to avoid shared references between source and destination
- Essential for proper memory management when trigger descriptors need to exist in multiple memory contexts
- Used primarily when copying trigger descriptors from working memory to cache memory context
- The copied structure is completely independent of the original and can be safely freed without affecting the source