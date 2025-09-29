# getObjectDescription

## Location
[src/backend/catalog/objectaddress.c:2903-4070](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L2903-L4070)

## Overview
Generates human-readable textual descriptions of PostgreSQL database objects for error messages and logging, providing localized string representations of any addressable object in the system catalog.

## Definition
```c
char *getObjectDescription(const ObjectAddress *object, bool missing_ok)
```

## Detailed Description
This function takes an ObjectAddress structure containing a class ID, object ID, and optional sub-object ID, and returns a pallocd string with a user-friendly description of the object. The function handles over 30 different types of database objects including relations, functions, types, constraints, triggers, roles, and many others. It uses a large switch statement to dispatch to appropriate formatting logic for each object class.

## Simplified Source

```c
char *getObjectDescription(const ObjectAddress *object, bool missing_ok)
{
    StringInfoData buffer;

    initStringInfo(&buffer);

    // Large switch statement handling different object classes
    switch (object->classId)
    {
        case RelationRelationId:
            // Handle relations (tables, views, etc.)
            if (object->objectSubId == 0)
                getRelationDescription(&buffer, object->objectId, missing_ok);
            else
            {
                // Handle individual columns
                char *attname = get_attname(object->objectId, object->objectSubId, missing_ok);
                if (!attname) break;

                StringInfoData rel;
                initStringInfo(&rel);
                getRelationDescription(&rel, object->objectId, missing_ok);
                appendStringInfo(&buffer, _("column %s of %s"), attname, rel.data);
                pfree(rel.data);
            }
            break;

        case ProcedureRelationId:
            // Handle functions/procedures
            {
                bits16 flags = FORMAT_PROC_INVALID_AS_NULL;
                char *proname = format_procedure_extended(object->objectId, flags);
                if (proname == NULL) break;
                appendStringInfo(&buffer, _("function %s"), proname);
            }
            break;

        case TypeRelationId:
            // Handle data types
            {
                bits16 flags = FORMAT_TYPE_INVALID_AS_NULL;
                char *typname = format_type_extended(object->objectId, -1, flags);
                if (typname == NULL) break;
                appendStringInfo(&buffer, _("type %s"), typname);
            }
            break;

        case CastRelationId:
            // Handle type casts - lookup in pg_cast
            {
                Relation castDesc = table_open(CastRelationId, AccessShareLock);
                // ... scan for cast record ...
                // appendStringInfo(&buffer, _("cast from %s to %s"), source, target);
                table_close(castDesc, AccessShareLock);
            }
            break;

        case ConstraintRelationId:
            // Handle table/domain constraints
            {
                HeapTuple conTup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object->objectId));
                if (!HeapTupleIsValid(conTup))
                {
                    if (!missing_ok) elog(ERROR, "cache lookup failed for constraint %u", object->objectId);
                    break;
                }
                Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(conTup);
                // Build constraint description based on whether it's table or domain constraint
                ReleaseSysCache(conTup);
            }
            break;

        case NamespaceRelationId:
            // Handle schemas
            {
                char *nspname = get_namespace_name(object->objectId);
                if (!nspname)
                {
                    if (!missing_ok) elog(ERROR, "cache lookup failed for namespace %u", object->objectId);
                    break;
                }
                appendStringInfo(&buffer, _("schema %s"), nspname);
            }
            break;

        case AuthIdRelationId:
            // Handle roles/users
            {
                char *username = GetUserNameFromId(object->objectId, missing_ok);
                if (username) appendStringInfo(&buffer, _("role %s"), username);
            }
            break;

        case DatabaseRelationId:
            // Handle databases
            {
                char *datname = get_database_name(object->objectId);
                if (!datname)
                {
                    if (!missing_ok) elog(ERROR, "cache lookup failed for database %u", object->objectId);
                    break;
                }
                appendStringInfo(&buffer, _("database %s"), datname);
            }
            break;

        // ... many more cases for other object types:
        // CollationRelationId, ConversionRelationId, LanguageRelationId,
        // OperatorRelationId, TriggerRelationId, ExtensionRelationId, etc.

        default:
            elog(ERROR, "unsupported object class: %u", object->classId);
    }

    // Return NULL if buffer is empty (object not found)
    if (buffer.len == 0)
        return NULL;

    return buffer.data;  // Caller must pfree() this
}
```