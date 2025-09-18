# doDeletion

## Location
src/backend/catalog/dependency.c: 1352 - 1495

## Overview
doDeletion is the central dispatch function that performs object-type-specific deletion logic, routing deletion requests to appropriate specialized functions based on the object's catalog class.

## Definition
```c
static void doDeletion(const ObjectAddress *object, int flags)
```

## Detailed Description
doDeletion implements a comprehensive switch statement that handles deletion for all supported PostgreSQL object types. The function examines the classId of the target object and routes the deletion to the appropriate specialized function. For relations, it provides special handling for different relation kinds (indexes, sequences, regular tables) and supports both concurrent and non-concurrent deletion modes. For most catalog objects, it delegates to specific removal functions, while using the generic DropObjectById for simpler catalog types. The function explicitly rejects deletion of global objects like databases and tablespaces that require special handling outside the dependency system.

## Parameters / Member Variables
- `object`: Pointer to ObjectAddress specifying the object to delete
  - `classId`: OID of the catalog relation, used for dispatching deletion logic
  - `objectId`: OID of the specific object to be deleted
  - `objectSubId`: Sub-object identifier (used for attributes, etc.)
- `flags`: Deletion behavior flags including:
  - PERFORM_DELETION_CONCURRENTLY: Enable concurrent deletion
  - PERFORM_DELETION_CONCURRENT_LOCK: Use concurrent locking mode

## Dependencies
- Functions called/Symbols referenced:
  - get_rel_relkind: Determines relation kind for relation objects
  - index_drop: Drops index objects with concurrent support
  - RemoveAttributeById: Removes table attributes/columns
  - heap_drop_with_catalog: Drops table relations
  - DeleteSequenceTuple: Removes sequence metadata
  - RemoveFunctionById: Removes function objects
  - RemoveTypeById: Removes data type objects
  - RemoveConstraintById: Removes constraint objects
  - RemoveAttrDefaultById: Removes attribute default objects
  - LargeObjectDrop: Removes large objects
  - RemoveOperatorById: Removes operator objects
  - RemoveRewriteRuleById: Removes rewrite rules
  - RemoveTriggerById: Removes trigger objects
  - RemoveStatisticsById: Removes statistics objects
  - RemoveTSConfigurationById: Removes text search configurations
  - RemoveExtensionById: Removes extension objects
  - RemovePolicyById: Removes row security policies
  - RemovePublicationSchemaById: Removes publication schemas
  - RemovePublicationRelById: Removes publication relations
  - RemovePublicationById: Removes publication objects
  - DropObjectById: Generic object deletion for simple catalog types
- Called from:
  - find_expr_references_context: Expression reference finding context
  - deleteOneObject: Main object deletion orchestration

## Notes and Other Information
- This function serves as the central routing mechanism for object deletion in PostgreSQL
- Handles special cases for relation objects, including concurrent index drops and sequence cleanup
- Provides comprehensive coverage for most PostgreSQL object types
- Explicitly prevents deletion of global objects (databases, tablespaces, etc.) that require special handling
- Uses specialized removal functions to ensure proper cleanup for complex object types
- Supports concurrent deletion modes for index objects
- The switch statement covers all major catalog relation IDs, with fallback error handling for unsupported types