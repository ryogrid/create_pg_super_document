# RelationBuildTriggers

## Location
src/backend/commands/trigger.c: 1856 - 2007

## Overview
RelationBuildTriggers builds trigger data to attach to the given relcache entry by scanning the pg_trigger system catalog and constructing a complete TriggerDesc structure for efficient trigger processing.

## Definition
void RelationBuildTriggers(Relation relation)

## Detailed Description
This function constructs trigger metadata for a given relation by performing the following operations:

1. **Memory Management Strategy**: Creates a temporary TriggerDesc structure in working memory context to avoid cache memory leaks if the operation fails partway through, then copies the completed structure to CacheMemoryContext for long-term storage.

2. **Catalog Scanning**: Scans the pg_trigger system catalog using TriggerRelidNameIndexId to find all triggers associated with the relation. The scan is performed in name order, ensuring triggers will be fired in alphabetical order.

3. **Trigger Structure Building**: For each trigger found, constructs a complete Trigger structure including:
   - Basic properties (OID, name, function OID, type, enabled status)
   - Constraint information (constraint relation, index, deferrable settings)
   - Attribute arrays for column-specific triggers
   - Argument arrays for trigger function parameters
   - Table transition names (OLD TABLE, NEW TABLE) for statement-level triggers
   - WHEN clause qualification expressions

4. **Dynamic Array Management**: Uses a dynamically resizable array starting with 16 slots that doubles in size when more triggers are found.

5. **Flag Setting**: Calls SetTriggerFlags() for each trigger to set appropriate trigger type flags in the TriggerDesc structure.

6. **Cache Integration**: Copies the completed trigger descriptor to cache memory and releases working memory.

## Parameters / Member Variables
- : The Relation structure for which to build trigger information

## Dependencies
- Functions called/Symbols referenced:
  - [SetTriggerFlags](../S/SetTriggerFlags.md): Sets trigger type flags in TriggerDesc
  - [CopyTriggerDesc](../C/CopyTriggerDesc.md): Copies TriggerDesc to cache memory context
  - [FreeTriggerDesc](../F/FreeTriggerDesc.md): Releases working memory for TriggerDesc
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext: System catalog scanning
  - [fastgetattr](../f/fastgetattr.md): Extracts attributes from HeapTuple
  - DirectFunctionCall1/nameout: Name conversion utilities
  - [DatumGetCString](../D/DatumGetCString.md)/TextDatumGetCString: Datum conversion utilities

- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md): During relation cache entry construction

## Notes and Other Information
- The function ensures triggers are processed in name order by using TriggerRelidNameIndexId
- Memory is carefully managed to prevent leaks in the cache context
- Handles variable-length fields like trigger arguments and attribute lists
- Returns early if no triggers are found for the relation
- The resulting TriggerDesc is stored in the relation's cache entry for efficient trigger execution
- Uses CacheMemoryContext for the final trigger descriptor to ensure it survives as long as the relcache entry