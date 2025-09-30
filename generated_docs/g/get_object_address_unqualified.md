# get_object_address_unqualified

## Location
[src/backend/catalog/objectaddress.c:1242-1332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1242-L1332)

## Overview
Finds an ObjectAddress for database objects that are identified by a simple unqualified name, handling various system-level PostgreSQL objects.

## Definition

```c
static ObjectAddress
get_object_address_unqualified(ObjectType objtype,
							   String *strval, bool missing_ok)
```
## Detailed Description
The  function is a static helper function that handles object address resolution for PostgreSQL objects that are identified by simple, unqualified names. These are typically system-level objects that exist in a global namespace rather than being schema-qualified. The function serves as a centralized dispatcher that maps object types to their corresponding catalog lookup functions.

The function performs a straightforward switch on the object type and calls the appropriate  function for each supported object type. Each case follows the same pattern: setting the appropriate catalog relation ID (classId), calling the type-specific lookup function, and setting objectSubId to 0 since these objects don't have sub-objects.

The supported object types include access methods, databases, extensions, tablespaces, roles, schemas, languages, foreign data wrappers, foreign servers, event triggers, parameter ACLs, publications, and subscriptions. These represent the major categories of PostgreSQL objects that exist at the cluster or database level rather than within specific schemas.

## Parameters / Member Variables
- : The type of object being looked up (from ObjectType enumeration)
- : String node containing the object name
- : If true, return invalid ObjectAddress instead of throwing error when object not found

## Dependencies
- Functions called/Symbols referenced:
  - [get_am_oid](get_am_oid.md) (for access methods)
  - [get_database_oid](get_database_oid.md) (for databases)
  - [get_extension_oid](get_extension_oid.md) (for extensions)
  - [get_tablespace_oid](get_tablespace_oid.md) (for tablespaces)
  - [get_role_oid](get_role_oid.md) (for roles)
  - [get_namespace_oid](get_namespace_oid.md) (for schemas)
  - [get_language_oid](get_language_oid.md) (for languages)
  - [get_foreign_data_wrapper_oid](get_foreign_data_wrapper_oid.md) (for foreign data wrappers)
  - [get_foreign_server_oid](get_foreign_server_oid.md) (for foreign servers)
  - [get_event_trigger_oid](get_event_trigger_oid.md) (for event triggers)
  - [ParameterAclLookup](../P/ParameterAclLookup.md) (for parameter ACLs)
  - [get_publication_oid](get_publication_oid.md) (for publications)
  - [get_subscription_oid](get_subscription_oid.md) (for subscriptions)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)

## Notes and Other Information
This function is marked static, indicating it's only used within the objectaddress.c module as a helper function. It handles objects that don't require schema qualification because they exist in global namespaces. The function follows a consistent pattern for all object types, making it easy to add new unqualified object types in the future. Each lookup function called by this function is responsible for its own error handling when missing_ok is false.

## Simplified Source

```c
static ObjectAddress
get_object_address_unqualified(ObjectType objtype,
                              String *strval, bool missing_ok)
{
    const char *name;
    ObjectAddress address;

    name = strVal(strval);

    // Dispatch to appropriate lookup function based on object type
    switch (objtype) {
        case OBJECT_ACCESS_METHOD:
            address.classId = AccessMethodRelationId;
            address.objectId = get_am_oid(name, missing_ok);
            break;
        case OBJECT_DATABASE:
            address.classId = DatabaseRelationId;
            address.objectId = get_database_oid(name, missing_ok);
            break;
        case OBJECT_EXTENSION:
            address.classId = ExtensionRelationId;
            address.objectId = get_extension_oid(name, missing_ok);
            break;
        case OBJECT_TABLESPACE:
            address.classId = TableSpaceRelationId;
            address.objectId = get_tablespace_oid(name, missing_ok);
            break;
        case OBJECT_ROLE:
            address.classId = AuthIdRelationId;
            address.objectId = get_role_oid(name, missing_ok);
            break;
        case OBJECT_SCHEMA:
            address.classId = NamespaceRelationId;
            address.objectId = get_namespace_oid(name, missing_ok);
            break;
        case OBJECT_LANGUAGE:
            address.classId = LanguageRelationId;
            address.objectId = get_language_oid(name, missing_ok);
            break;
        case OBJECT_FDW:
            address.classId = ForeignDataWrapperRelationId;
            address.objectId = get_foreign_data_wrapper_oid(name, missing_ok);
            break;
        case OBJECT_FOREIGN_SERVER:
            address.classId = ForeignServerRelationId;
            address.objectId = get_foreign_server_oid(name, missing_ok);
            break;
        case OBJECT_EVENT_TRIGGER:
            address.classId = EventTriggerRelationId;
            address.objectId = get_event_trigger_oid(name, missing_ok);
            break;
        case OBJECT_PARAMETER_ACL:
            address.classId = ParameterAclRelationId;
            address.objectId = ParameterAclLookup(name, missing_ok);
            break;
        case OBJECT_PUBLICATION:
            address.classId = PublicationRelationId;
            address.objectId = get_publication_oid(name, missing_ok);
            break;
        case OBJECT_SUBSCRIPTION:
            address.classId = SubscriptionRelationId;
            address.objectId = get_subscription_oid(name, missing_ok);
            break;
        default:
            elog(ERROR, "unrecognized object type: %d", (int) objtype);
            // Fallback values (never reached)
            address.classId = InvalidOid;
            address.objectId = InvalidOid;
    }

    address.objectSubId = 0;  // All these objects have no sub-objects
    return address;
}
```