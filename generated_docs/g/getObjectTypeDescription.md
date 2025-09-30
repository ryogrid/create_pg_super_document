# getObjectTypeDescription

## Location
[src/backend/catalog/objectaddress.c:4413-4602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4413-L4602)

## Overview
Returns a human-readable string that describes the type of PostgreSQL database object specified by an ObjectAddress, supporting all major object classes in the system catalog.

## Definition
```c
char *getObjectTypeDescription(const ObjectAddress *object, bool missing_ok)
```

## Detailed Description
This function provides human-readable type descriptions for PostgreSQL database objects. It takes an ObjectAddress structure and returns a palloc'ed string containing the object type description. The function uses a large switch statement to map catalog relation OIDs to descriptive strings.

For certain complex object types (relations, procedures, constraints), it delegates to specialized helper functions that provide more detailed type information. For simpler object types, it returns static string descriptions.

The function supports all major PostgreSQL object classes including relations, functions, types, operators, access methods, text search objects, security objects, and replication objects. It ensures that a valid description is always returned for supported object classes.

## Parameters / Member Variables
- `object` (const ObjectAddress *): Pointer to ObjectAddress structure containing classId, objectId, and objectSubId
- `missing_ok` (bool): Whether to tolerate missing objects (passed to helper functions)

## Dependencies
- Functions called/Symbols referenced:
  - [getRelationTypeDescription](getRelationTypeDescription.md)
  - [getProcedureTypeDescription](getProcedureTypeDescription.md)  
  - [getConstraintTypeDescription](getConstraintTypeDescription.md)
- Called from (representative examples):
  - [pg_identify_object](../p/pg_identify_object.md)
  - [pg_identify_object_as_address](../p/pg_identify_object_as_address.md)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md)
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md)
  - ObjectAddressSet

## Notes and Other Information
- Returns a palloc'ed string that must be freed by the caller
- The ObjectTypeMap should be kept in sync with this function's switch statement
- Throws an error for unsupported object classes
- For complex object types, delegates to specialized functions for detailed type information
- Located in src/backend/catalog/objectaddress.c:4413-4602
- Supports all major PostgreSQL object types including tables, indexes, functions, types, operators, etc.

## Simplified Source
```c
char *
getObjectTypeDescription(const ObjectAddress *object, bool missing_ok)
{
    StringInfoData buffer;
    initStringInfo(&buffer);

    switch (object->classId)
    {
        // Complex types with specialized handling
        case RelationRelationId:
            getRelationTypeDescription(&buffer, object->objectId, object->objectSubId, missing_ok);
            break;

        case ProcedureRelationId:
            getProcedureTypeDescription(&buffer, object->objectId, missing_ok);
            break;

        case ConstraintRelationId:
            getConstraintTypeDescription(&buffer, object->objectId, missing_ok);
            break;

        // Simple static descriptions
        case TypeRelationId:            appendStringInfoString(&buffer, "type"); break;
        case CastRelationId:            appendStringInfoString(&buffer, "cast"); break;
        case CollationRelationId:       appendStringInfoString(&buffer, "collation"); break;
        case ConversionRelationId:      appendStringInfoString(&buffer, "conversion"); break;
        case AttrDefaultRelationId:     appendStringInfoString(&buffer, "default value"); break;
        case LanguageRelationId:        appendStringInfoString(&buffer, "language"); break;
        case LargeObjectRelationId:     appendStringInfoString(&buffer, "large object"); break;
        case OperatorRelationId:        appendStringInfoString(&buffer, "operator"); break;
        case OperatorClassRelationId:   appendStringInfoString(&buffer, "operator class"); break;
        case OperatorFamilyRelationId:  appendStringInfoString(&buffer, "operator family"); break;
        case AccessMethodRelationId:    appendStringInfoString(&buffer, "access method"); break;
        case AccessMethodOperatorRelationId: appendStringInfoString(&buffer, "operator of access method"); break;
        case AccessMethodProcedureRelationId: appendStringInfoString(&buffer, "function of access method"); break;
        case RewriteRelationId:         appendStringInfoString(&buffer, "rule"); break;
        case TriggerRelationId:         appendStringInfoString(&buffer, "trigger"); break;
        case NamespaceRelationId:       appendStringInfoString(&buffer, "schema"); break;
        case StatisticExtRelationId:    appendStringInfoString(&buffer, "statistics object"); break;

        // Text search objects
        case TSParserRelationId:        appendStringInfoString(&buffer, "text search parser"); break;
        case TSDictionaryRelationId:    appendStringInfoString(&buffer, "text search dictionary"); break;
        case TSTemplateRelationId:      appendStringInfoString(&buffer, "text search template"); break;
        case TSConfigRelationId:        appendStringInfoString(&buffer, "text search configuration"); break;

        // Security and user objects
        case AuthIdRelationId:          appendStringInfoString(&buffer, "role"); break;
        case AuthMemRelationId:         appendStringInfoString(&buffer, "role membership"); break;
        case ParameterAclRelationId:    appendStringInfoString(&buffer, "parameter ACL"); break;
        case PolicyRelationId:          appendStringInfoString(&buffer, "policy"); break;
        case DefaultAclRelationId:      appendStringInfoString(&buffer, "default acl"); break;

        // Infrastructure objects
        case DatabaseRelationId:        appendStringInfoString(&buffer, "database"); break;
        case TableSpaceRelationId:      appendStringInfoString(&buffer, "tablespace"); break;
        case ExtensionRelationId:       appendStringInfoString(&buffer, "extension"); break;
        case EventTriggerRelationId:    appendStringInfoString(&buffer, "event trigger"); break;

        // Foreign data objects
        case ForeignDataWrapperRelationId: appendStringInfoString(&buffer, "foreign-data wrapper"); break;
        case ForeignServerRelationId:   appendStringInfoString(&buffer, "server"); break;
        case UserMappingRelationId:     appendStringInfoString(&buffer, "user mapping"); break;

        // Replication objects
        case PublicationRelationId:         appendStringInfoString(&buffer, "publication"); break;
        case PublicationNamespaceRelationId: appendStringInfoString(&buffer, "publication namespace"); break;
        case PublicationRelRelationId:       appendStringInfoString(&buffer, "publication relation"); break;
        case SubscriptionRelationId:        appendStringInfoString(&buffer, "subscription"); break;
        case TransformRelationId:            appendStringInfoString(&buffer, "transform"); break;

        default:
            elog(ERROR, "unsupported object class: %u", object->classId);
    }

    Assert(buffer.len > 0);
    return buffer.data;
}
```