# doDeletion

## Location
[src/backend/catalog/dependency.c:1352-1495](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1352-L1495)

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
  - [get_rel_relkind](../g/get_rel_relkind.md): Determines relation kind for relation objects
  - [index_drop](../i/index_drop.md): Drops index objects with concurrent support
  - [RemoveAttributeById](../R/RemoveAttributeById.md): Removes table attributes/columns
  - [heap_drop_with_catalog](../h/heap_drop_with_catalog.md): Drops table relations
  - [DeleteSequenceTuple](../D/DeleteSequenceTuple.md): Removes sequence metadata
  - [RemoveFunctionById](../R/RemoveFunctionById.md): Removes function objects
  - [RemoveTypeById](../R/RemoveTypeById.md): Removes data type objects
  - [RemoveConstraintById](../R/RemoveConstraintById.md): Removes constraint objects
  - [RemoveAttrDefaultById](../R/RemoveAttrDefaultById.md): Removes attribute default objects
  - [LargeObjectDrop](../L/LargeObjectDrop.md): Removes large objects
  - [RemoveOperatorById](../R/RemoveOperatorById.md): Removes operator objects
  - [RemoveRewriteRuleById](../R/RemoveRewriteRuleById.md): Removes rewrite rules
  - [RemoveTriggerById](../R/RemoveTriggerById.md): Removes trigger objects
  - [RemoveStatisticsById](../R/RemoveStatisticsById.md): Removes statistics objects
  - [RemoveTSConfigurationById](../R/RemoveTSConfigurationById.md): Removes text search configurations
  - [RemoveExtensionById](../R/RemoveExtensionById.md): Removes extension objects
  - [RemovePolicyById](../R/RemovePolicyById.md): Removes row security policies
  - [RemovePublicationSchemaById](../R/RemovePublicationSchemaById.md): Removes publication schemas
  - [RemovePublicationRelById](../R/RemovePublicationRelById.md): Removes publication relations
  - [RemovePublicationById](../R/RemovePublicationById.md): Removes publication objects
  - [DropObjectById](../D/DropObjectById.md): Generic object deletion for simple catalog types
- Called from:
  - find_expr_references_context: Expression reference finding context
  - [deleteOneObject](deleteOneObject.md): Main object deletion orchestration

## Notes and Other Information
- This function serves as the central routing mechanism for object deletion in PostgreSQL
- Handles special cases for relation objects, including concurrent index drops and sequence cleanup
- Provides comprehensive coverage for most PostgreSQL object types
- Explicitly prevents deletion of global objects (databases, tablespaces, etc.) that require special handling
- Uses specialized removal functions to ensure proper cleanup for complex object types
- Supports concurrent deletion modes for index objects
- The switch statement covers all major catalog relation IDs, with fallback error handling for unsupported types

## Simplified Source

```c
static void doDeletion(const ObjectAddress *object, int flags) {
    switch (object->classId) {
        case RelationRelationId:
            {
                char relKind = get_rel_relkind(object->objectId);

                // Handle indexes and partitioned indexes
                if (relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX) {
                    bool concurrent = ((flags & PERFORM_DELETION_CONCURRENTLY) != 0);
                    bool concurrent_lock = ((flags & PERFORM_DELETION_CONCURRENT_LOCK) != 0);
                    index_drop(object->objectId, concurrent, concurrent_lock);
                } else {
                    // Handle table attributes or entire relations
                    if (object->objectSubId != 0)
                        RemoveAttributeById(object->objectId, object->objectSubId);
                    else
                        heap_drop_with_catalog(object->objectId);
                }

                // Special cleanup for sequences
                if (relKind == RELKIND_SEQUENCE)
                    DeleteSequenceTuple(object->objectId);
                break;
            }

        // Functions and procedures
        case ProcedureRelationId:
            RemoveFunctionById(object->objectId);
            break;

        // Data types
        case TypeRelationId:
            RemoveTypeById(object->objectId);
            break;

        // Constraints
        case ConstraintRelationId:
            RemoveConstraintById(object->objectId);
            break;

        // Attribute defaults
        case AttrDefaultRelationId:
            RemoveAttrDefaultById(object->objectId);
            break;

        // Large objects
        case LargeObjectRelationId:
            LargeObjectDrop(object->objectId);
            break;

        // Core database objects with specialized removal functions
        case OperatorRelationId:
            RemoveOperatorById(object->objectId);
            break;
        case RewriteRelationId:
            RemoveRewriteRuleById(object->objectId);
            break;
        case TriggerRelationId:
            RemoveTriggerById(object->objectId);
            break;
        case StatisticExtRelationId:
            RemoveStatisticsById(object->objectId);
            break;
        case TSConfigRelationId:
            RemoveTSConfigurationById(object->objectId);
            break;
        case ExtensionRelationId:
            RemoveExtensionById(object->objectId);
            break;
        case PolicyRelationId:
            RemovePolicyById(object->objectId);
            break;

        // Publication objects
        case PublicationNamespaceRelationId:
            RemovePublicationSchemaById(object->objectId);
            break;
        case PublicationRelRelationId:
            RemovePublicationRelById(object->objectId);
            break;
        case PublicationRelationId:
            RemovePublicationById(object->objectId);
            break;

        // Simple catalog objects using generic deletion
        case CastRelationId:
        case CollationRelationId:
        case ConversionRelationId:
        case LanguageRelationId:
        case OperatorClassRelationId:
        case OperatorFamilyRelationId:
        case AccessMethodRelationId:
        case AccessMethodOperatorRelationId:
        case AccessMethodProcedureRelationId:
        case NamespaceRelationId:
        case TSParserRelationId:
        case TSDictionaryRelationId:
        case TSTemplateRelationId:
        case ForeignDataWrapperRelationId:
        case ForeignServerRelationId:
        case UserMappingRelationId:
        case DefaultAclRelationId:
        case EventTriggerRelationId:
        case TransformRelationId:
        case AuthMemRelationId:
            DropObjectById(object);
            break;

        // Global objects are not supported
        case AuthIdRelationId:
        case DatabaseRelationId:
        case TableSpaceRelationId:
        case SubscriptionRelationId:
        case ParameterAclRelationId:
            elog(ERROR, "global objects cannot be deleted by doDeletion");
            break;

        default:
            elog(ERROR, "unsupported object class: %u", object->classId);
    }
}
```