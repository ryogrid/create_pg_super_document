# FreeTriggerDesc

## Location
[src/backend/commands/trigger.c:2140-2176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2140-L2176)

## Overview
FreeTriggerDesc deallocates a TriggerDesc data structure and all its associated memory, including all variable-length fields within each trigger.

## Definition
void FreeTriggerDesc(TriggerDesc *trigdesc)

## Detailed Description
This function performs comprehensive memory deallocation for a TriggerDesc structure and all its components. The function systematically frees memory in the following order:

1. **Null Check**: Returns immediately if the input TriggerDesc is NULL, providing safe handling of null pointers.

2. **Trigger-by-Trigger Deallocation**: Iterates through each trigger in the triggers array and frees all dynamically allocated fields:
   - **tgname**: Trigger name string
   - **tgattr**: Array of column attribute numbers (if present)
   - **tgargs**: Array of trigger function argument strings (frees each string individually, then the array)
   - **tgqual**: WHEN clause qualification expression string (if present)
   - **tgoldtable**: OLD TABLE transition table name (if present)
   - **tgnewtable**: NEW TABLE transition table name (if present)

3. **Array Deallocation**: For the tgargs array, it decrements the argument count while freeing each individual argument string, ensuring no memory leaks.

4. **Structure Deallocation**: Finally frees the trigger array itself and the TriggerDesc structure.

The function ensures complete cleanup of all memory allocated by RelationBuildTriggers or CopyTriggerDesc, preventing memory leaks in trigger processing.

## Parameters / Member Variables
- : Pointer to TriggerDesc structure to deallocate (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md): Memory deallocation function

- Called from (representative examples):
  - [RelationBuildTriggers](../R/RelationBuildTriggers.md): To clean up working memory after copying to cache
  - [RelationDestroyRelation](../R/RelationDestroyRelation.md): During relation cache entry cleanup

## Notes and Other Information
- Safe to call with NULL pointer - function returns immediately without error
- Frees all dynamically allocated memory within the TriggerDesc structure
- Uses a careful approach for freeing tgargs array by decrementing the count and freeing strings individually
- Essential for preventing memory leaks in trigger descriptor management
- Typically called in cleanup scenarios after trigger descriptors are no longer needed
- Works as the counterpart to RelationBuildTriggers and CopyTriggerDesc for complete memory lifecycle management