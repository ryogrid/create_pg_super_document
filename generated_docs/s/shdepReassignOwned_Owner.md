# shdepReassignOwned_Owner

## Location
[src/backend/catalog/pg_shdepend.c:1647-1733](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1647-L1733)

## Overview
A static helper function that handles ownership reassignment for SHARED_DEPENDENCY_OWNER entries by dispatching to appropriate ALTER OWNER functions based on object class.

## Definition
```c
static void shdepReassignOwned_Owner(Form_pg_shdepend sdepForm, Oid newrole)
```

## Detailed Description
This function is a dispatch routine that examines the class ID of a database object and calls the appropriate ownership transfer function. It handles reassignment for various PostgreSQL object types including:

- **Types**: Calls AlterTypeOwner_oid
- **Schemas**: Calls AlterSchemaOwner_oid  
- **Relations (tables, views, etc.)**: Calls ATExecChangeOwner with recursing=true to handle dependent objects
- **Foreign servers**: Calls AlterForeignServerOwner_oid
- **Foreign data wrappers**: Calls AlterForeignDataWrapperOwner_oid
- **Event triggers**: Calls AlterEventTriggerOwner_oid
- **Publications**: Calls AlterPublicationOwner_oid
- **Subscriptions**: Calls AlterSubscriptionOwner_oid
- **Generic objects**: Uses AlterObjectOwner_internal for collations, conversions, operators, procedures, languages, large objects, operator families/classes, extensions, statistics, tablespaces, databases, and text search objects

Some object types like default ACLs and user mappings are explicitly ignored as they should be handled by DROP OWNED rather than REASSIGN OWNED.

## Parameters / Member Variables
- `sdepForm`: Pointer to pg_shdepend tuple containing the dependency information (classid, objid, objsubid)
- `newrole`: OID of the role that will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [AlterTypeOwner_oid](../A/AlterTypeOwner_oid.md) - Changes ownership of user-defined types
  - [AlterSchemaOwner_oid](../A/AlterSchemaOwner_oid.md) - Changes ownership of schemas
  - [ATExecChangeOwner](../A/ATExecChangeOwner.md) - Changes ownership of relations (tables, views, etc.)
  - [AlterForeignServerOwner_oid](../A/AlterForeignServerOwner_oid.md) - Changes ownership of foreign servers
  - [AlterForeignDataWrapperOwner_oid](../A/AlterForeignDataWrapperOwner_oid.md) - Changes ownership of foreign data wrappers
  - [AlterEventTriggerOwner_oid](../A/AlterEventTriggerOwner_oid.md) - Changes ownership of event triggers
  - [AlterPublicationOwner_oid](../A/AlterPublicationOwner_oid.md) - Changes ownership of publications
  - [AlterSubscriptionOwner_oid](../A/AlterSubscriptionOwner_oid.md) - Changes ownership of subscriptions
  - [AlterObjectOwner_internal](../A/AlterObjectOwner_internal.md) - Generic ownership change function for many object types
  - AccessExclusiveLock - Lock level constant used for relation ownership changes

- Called from (representative examples):
  - [shdepReassignOwned](shdepReassignOwned.md) (src/backend/catalog/pg_shdepend.c:1611)
  - ShDependObjectInfo (src/backend/catalog/pg_shdepend.c:106)

## Notes and Other Information
- Static function only used within pg_shdepend.c
- Uses recursing=true for ATExecChangeOwner to handle dependent objects like indexes and sequences
- Explicitly ignores DefaultAclRelationId and UserMappingRelationId objects
- Part of the ownership reassignment infrastructure called during REASSIGN OWNED operations
- Centralizes object type-specific ownership change logic in one location

## Simplified Source

```c
static void
shdepReassignOwned_Owner(Form_pg_shdepend sdepForm, Oid newrole)
{
    // Dispatch to appropriate ALTER OWNER function based on object class
    switch (sdepForm->classid) {
        case TypeRelationId:
            AlterTypeOwner_oid(sdepForm->objid, newrole, true);
            break;

        case NamespaceRelationId:
            AlterSchemaOwner_oid(sdepForm->objid, newrole);
            break;

        case RelationRelationId:
            // Use recursing=true to handle dependent objects (indexes, sequences)
            ATExecChangeOwner(sdepForm->objid, newrole, true, AccessExclusiveLock);
            break;

        case DefaultAclRelationId:
        case UserMappingRelationId:
            // Skip - handled by DROP OWNED, not REASSIGN OWNED
            break;

        case ForeignServerRelationId:
            AlterForeignServerOwner_oid(sdepForm->objid, newrole);
            break;

        case ForeignDataWrapperRelationId:
            AlterForeignDataWrapperOwner_oid(sdepForm->objid, newrole);
            break;

        case EventTriggerRelationId:
            AlterEventTriggerOwner_oid(sdepForm->objid, newrole);
            break;

        case PublicationRelationId:
            AlterPublicationOwner_oid(sdepForm->objid, newrole);
            break;

        case SubscriptionRelationId:
            AlterSubscriptionOwner_oid(sdepForm->objid, newrole);
            break;

        // Generic cases for multiple object types
        case CollationRelationId:
        case ConversionRelationId:
        case OperatorRelationId:
        case ProcedureRelationId:
        case LanguageRelationId:
        case LargeObjectRelationId:
        case OperatorFamilyRelationId:
        case OperatorClassRelationId:
        case ExtensionRelationId:
        case StatisticExtRelationId:
        case TableSpaceRelationId:
        case DatabaseRelationId:
        case TSConfigRelationId:
        case TSDictionaryRelationId:
            AlterObjectOwner_internal(sdepForm->classid, sdepForm->objid, newrole);
            break;

        default:
            elog(ERROR, "unexpected classid %u", sdepForm->classid);
            break;
    }
}
```