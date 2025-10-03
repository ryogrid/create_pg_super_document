# does_not_exist_skipping

## Location
[src/backend/commands/dropcmds.c:243-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dropcmds.c#L243-L524)

## Overview
does_not_exist_skipping generates appropriate NOTICE messages when objects specified in DROP IF EXISTS statements are not found, implementing intelligent error reporting that distinguishes between missing objects and missing dependencies.

## Definition

```c
static void
does_not_exist_skipping(ObjectType objtype, Node *object)
```
## Detailed Description
This function is the central dispatcher for generating NOTICE messages when objects don't exist in DROP IF EXISTS operations. It contains a large switch statement that handles different object types, using hierarchical checking through helper functions to determine whether the object itself is missing or its dependencies (schemas, types, owning relations) are missing. The function provides user-friendly error messages that accurately describe what is missing and being skipped.

## Parameters / Member Variables
- `objtype`: ObjectType enum value indicating the type of object being dropped (OBJECT_FUNCTION, OBJECT_TYPE, etc.)
- `*object`: Node pointer containing the object specification (name, arguments, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [schema_does_not_exist_skipping](../s/schema_does_not_exist_skipping.md): Checks if schema exists for schema-qualified objects
  - [type_in_list_does_not_exist_skipping](../t/type_in_list_does_not_exist_skipping.md): Checks if types exist for objects with type dependencies
  - [owningrel_does_not_exist_skipping](../o/owningrel_does_not_exist_skipping.md): Checks if owning relation exists for triggers/rules/policies
  - [TypeNameToString](../T/TypeNameToString.md): Converts TypeName to string for messages
  - [NameListToString](../N/NameListToString.md): Converts name lists to strings for messages
  - [TypeNameListToString](../T/TypeNameListToString.md): Converts type argument lists to strings
  - Various list manipulation functions (list_copy_head, list_copy_tail, etc.)

- Called from (representative examples):
  - [RemoveObjects](../R/RemoveObjects.md): Main caller when objects are not found during DROP operations

## Notes and Other Information
- This is a static function internal to dropcmds.c
- Only relevant when missing_ok is true (IF EXISTS clause)
- Handles 20+ different object types with appropriate error messages
- Uses hierarchical checking to provide precise error information
- Some object types (relations, roles, etc.) are handled elsewhere and trigger errors if passed here
- Supports objects with complex specifications (functions with arguments, casts between types)
- Always generates NOTICE level messages, never ERROR (since IF EXISTS allows missing objects)
- Messages are internationalized using gettext_noop

## Simplified Source

```c
static void does_not_exist_skipping(ObjectType objtype, Node *object) {
    const char *msg = NULL;
    char *name = NULL;
    char *args = NULL;

    // Main switch statement handles different object types
    switch (objtype) {
        // Simple object types (schema, extension, language, etc.)
        case OBJECT_ACCESS_METHOD:
            msg = gettext_noop("access method \"%s\" does not exist, skipping");
            name = strVal(object);
            break;

        case OBJECT_SCHEMA:
            msg = gettext_noop("schema \"%s\" does not exist, skipping");
            name = strVal(object);
            break;

        case OBJECT_EXTENSION:
            msg = gettext_noop("extension \"%s\" does not exist, skipping");
            name = strVal(object);
            break;

        // Types (domain, type) - check schema first
        case OBJECT_TYPE:
        case OBJECT_DOMAIN:
            if (!schema_does_not_exist_skipping(typ->names, &msg, &name)) {
                msg = gettext_noop("type \"%s\" does not exist, skipping");
                name = TypeNameToString(typ);
            }
            break;

        // Functions/procedures/aggregates - check schema and argument types
        case OBJECT_FUNCTION:
        case OBJECT_PROCEDURE:
        case OBJECT_AGGREGATE:
            if (!schema_does_not_exist_skipping(owa->objname, &msg, &name) &&
                !type_in_list_does_not_exist_skipping(owa->objargs, &msg, &name)) {
                msg = gettext_noop("function %s(%s) does not exist, skipping");
                name = NameListToString(owa->objname);
                args = TypeNameListToString(owa->objargs);
            }
            break;

        // Relation-dependent objects (triggers, rules, policies)
        case OBJECT_TRIGGER:
        case OBJECT_RULE:
        case OBJECT_POLICY:
            if (!owningrel_does_not_exist_skipping(object_list, &msg, &name)) {
                msg = gettext_noop("trigger \"%s\" for relation \"%s\" does not exist, skipping");
                name = strVal(llast(object_list));
                args = NameListToString(list_copy_head(object_list, list_length(object_list) - 1));
            }
            break;

        // Casts - check both source and target types
        case OBJECT_CAST:
            if (!type_in_list_does_not_exist_skipping(source_type, &msg, &name) &&
                !type_in_list_does_not_exist_skipping(target_type, &msg, &name)) {
                msg = gettext_noop("cast from type %s to type %s does not exist, skipping");
                name = TypeNameToString(source_type);
                args = TypeNameToString(target_type);
            }
            break;

        // Other object types handled similarly...
        default:
            elog(ERROR, "unsupported object type: %d", (int) objtype);
            break;
    }

    // Generate the NOTICE message
    if (!msg)
        elog(ERROR, "unrecognized object type: %d", (int) objtype);

    if (!args)
        ereport(NOTICE, (errmsg(msg, name)));
    else
        ereport(NOTICE, (errmsg(msg, name, args)));
}
```