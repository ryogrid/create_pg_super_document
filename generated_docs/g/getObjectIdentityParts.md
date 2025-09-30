# getObjectIdentityParts

## Location
[src/backend/catalog/objectaddress.c:4755-5964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4755-L5964)

## Overview
A comprehensive function that generates detailed identity information for database objects, returning both a complete identity string and optionally decomposed object name and argument lists suitable for reconstructing the ObjectAddress.

## Definition
```c
char *getObjectIdentityParts(const ObjectAddress *object, List **objname, List **objargs, bool missing_ok)
```

## Detailed Description
This function is the core implementation for object identity generation in PostgreSQL, handling all major database object types through a comprehensive switch statement based on the object's class ID. It constructs a human-readable identity string while optionally providing decomposed components that can be used with get_object_address() to reconstruct the original ObjectAddress.

The function handles over 30 different object types including relations, procedures, types, casts, collations, constraints, conversions, languages, operators, access methods, namespaces, users, databases, extensions, and many others. For each object type, it performs catalog lookups to retrieve the necessary information and formats it appropriately with schema qualification when needed.

The dual return mechanism allows for both display purposes (the string) and programmatic reconstruction (the lists), making it suitable for various use cases including object identification, event triggers, and system catalog operations.

## Parameters / Member Variables
- `object`: Pointer to an ObjectAddress structure containing the object's class ID, object ID, and sub-object ID
- `objname`: Output parameter for a list of C-strings representing the object name components (can be NULL)
- `objargs`: Output parameter for a list of C-strings representing the object arguments (can be NULL)
- `missing_ok`: Boolean flag indicating whether to handle missing objects gracefully (true) or raise an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [get_attname](get_attname.md) (attribute name lookup)
  - [getRelationIdentity](getRelationIdentity.md) (relation identity formatting)
  - [format_procedure_extended](../f/format_procedure_extended.md) (procedure formatting)
  - [format_type_extended](../f/format_type_extended.md) (type formatting)
  - [format_operator_extended](../f/format_operator_extended.md) (operator formatting)
  - [getOpFamilyIdentity](getOpFamilyIdentity.md) (operator family identity)
  - [GetAttrDefaultColumnAddress](../G/GetAttrDefaultColumnAddress.md) (attribute default lookup)
  - [LargeObjectExists](../L/LargeObjectExists.md) (large object existence check)
  - [GetForeignDataWrapperExtended](../G/GetForeignDataWrapperExtended.md) (FDW lookup)
  - [GetForeignServerExtended](../G/GetForeignServerExtended.md) (foreign server lookup)
  - [GetUserNameFromId](../G/GetUserNameFromId.md) (user name lookup)
  - [get_namespace_name_or_temp](get_namespace_name_or_temp.md) (namespace name lookup)
  - [get_database_name](get_database_name.md) (database name lookup)
  - [get_tablespace_name](get_tablespace_name.md) (tablespace name lookup)
  - [get_extension_name](get_extension_name.md) (extension name lookup)
  - [get_publication_name](get_publication_name.md) (publication name lookup)
  - [get_subscription_name](get_subscription_name.md) (subscription name lookup)
  - [getPublicationSchemaInfo](getPublicationSchemaInfo.md) (publication schema information)
  - [quote_identifier](../q/quote_identifier.md) (identifier quoting)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md) (qualified identifier quoting)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - [get_catalog_object_by_oid](get_catalog_object_by_oid.md) (catalog object retrieval)
  - Various catalog form structures (Form_pg_*)

- Called from (representative examples):
  - [getObjectIdentity](getObjectIdentity.md) (simplified interface wrapper)
  - [pg_identify_object_as_address](../p/pg_identify_object_as_address.md) (SQL function for address-based identification)
  - [EventTriggerSQLDropAddObject](../E/EventTriggerSQLDropAddObject.md) (event trigger system)
  - ObjectAddressSet (object address utility)

## Notes and Other Information
- This is a very large function (over 1200 lines) that serves as the central dispatcher for object identity generation
- The function uses extensive catalog lookups and system cache operations for performance
- Proper error handling with missing_ok parameter allows graceful degradation when objects don't exist
- Schema qualification is automatically applied when necessary for unambiguous identification
- The function supports recursive calls for complex objects like constraints on domains
- Memory management follows PostgreSQL conventions with palloc'd strings that must be freed by caller
- Output lists use PostgreSQL's List structure and associated utility functions like list_make1, list_make2, etc.
- Part of PostgreSQL's comprehensive object address and identification infrastructure
- Critical component for event triggers, dependency tracking, and object management operations

## Simplified Source
```c
char *
getObjectIdentityParts(const ObjectAddress *object, List **objname, List **objargs, bool missing_ok)
{
    StringInfoData buffer;
    initStringInfo(&buffer);

    // Initialize output parameters if provided
    if (objname)
    {
        *objname = NIL;
        *objargs = NIL;
    }

    // Main switch statement handling all object types
    switch (object->classId)
    {
        case RelationRelationId:
            // Handle tables, views, sequences, etc. (with optional column reference)
            if (object->objectSubId != 0)
            {
                char *attr = get_attname(object->objectId, object->objectSubId, missing_ok);
                if (missing_ok && attr == NULL) break;
            }
            getRelationIdentity(&buffer, object->objectId, objname, missing_ok);
            // Add column name if specified
            break;

        case ProcedureRelationId:
            // Handle functions and procedures
            char *proname = format_procedure_extended(object->objectId, FORMAT_PROC_FORCE_QUALIFY | FORMAT_PROC_INVALID_AS_NULL);
            if (proname) {
                appendStringInfoString(&buffer, proname);
                if (objname) format_procedure_parts(object->objectId, objname, objargs, missing_ok);
            }
            break;

        case TypeRelationId:
            // Handle data types
            char *typeout = format_type_extended(object->objectId, -1, FORMAT_TYPE_INVALID_AS_NULL | FORMAT_TYPE_FORCE_QUALIFY);
            if (typeout) {
                appendStringInfoString(&buffer, typeout);
                if (objname) *objname = list_make1(typeout);
            }
            break;

        case ConstraintRelationId:
            // Handle table and domain constraints
            HeapTuple conTup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object->objectId));
            if (HeapTupleIsValid(conTup)) {
                Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);
                if (OidIsValid(con->conrelid)) {
                    // Table constraint
                    appendStringInfo(&buffer, "%s on ", quote_identifier(NameStr(con->conname)));
                    getRelationIdentity(&buffer, con->conrelid, objname, false);
                } else {
                    // Domain constraint
                    ObjectAddress domain = {TypeRelationId, con->contypid, 0};
                    appendStringInfo(&buffer, "%s on %s", quote_identifier(NameStr(con->conname)),
                                   getObjectIdentityParts(&domain, objname, objargs, false));
                }
                ReleaseSysCache(conTup);
            }
            break;

        case LanguageRelationId:
            // Handle procedural languages
            HeapTuple langTup = SearchSysCache1(LANGOID, ObjectIdGetDatum(object->objectId));
            if (HeapTupleIsValid(langTup)) {
                Form_pg_language langForm = (Form_pg_language) GETSTRUCT(langTup);
                appendStringInfoString(&buffer, quote_identifier(NameStr(langForm->lanname)));
                if (objname) *objname = list_make1(pstrdup(NameStr(langForm->lanname)));
                ReleaseSysCache(langTup);
            }
            break;

        case NamespaceRelationId:
            // Handle schemas/namespaces
            char *nspname = get_namespace_name_or_temp(object->objectId);
            if (nspname) {
                appendStringInfoString(&buffer, quote_identifier(nspname));
                if (objname) *objname = list_make1(nspname);
            }
            break;

        case AuthIdRelationId:
            // Handle users/roles
            char *username = GetUserNameFromId(object->objectId, missing_ok);
            if (username) {
                appendStringInfoString(&buffer, quote_identifier(username));
                if (objname) *objname = list_make1(username);
            }
            break;

        case DatabaseRelationId:
            // Handle databases
            char *datname = get_database_name(object->objectId);
            if (datname) {
                appendStringInfoString(&buffer, quote_identifier(datname));
                if (objname) *objname = list_make1(datname);
            }
            break;

        // ... Additional cases for other object types follow similar patterns:
        // - Look up object info from system catalogs
        // - Format identity string with proper quoting
        // - Build objname/objargs lists if requested
        // - Handle missing_ok flag for graceful failure

        default:
            if (!missing_ok)
                elog(ERROR, "unsupported object class: %u", object->classId);
    }

    // Validate that we produced output if not in missing_ok mode
    if (!missing_ok && objname && *objname == NIL)
        elog(ERROR, "requested object address for unsupported object class %u", object->classId);

    // Return NULL if no object found and missing_ok is true
    if (missing_ok && buffer.len == 0)
        return NULL;

    return buffer.data;
}
```