# DeconstructQualifiedName

## Location
[src/backend/catalog/namespace.c:3301-3354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3301-L3354)

## Overview
Parses a possibly-qualified name expressed as a list of String nodes and extracts the schema name and object name components.

## Definition
```c
void DeconstructQualifiedName(const List *names, char **nspname_p, char **objname_p)
```

## Detailed Description
DeconstructQualifiedName takes a list of String nodes representing a qualified name (like "schema.table" or "database.schema.table") and breaks it down into its component parts. It handles 1, 2, or 3-part names: unqualified names (object only), schema-qualified names (schema.object), and fully-qualified names (database.schema.object). For 3-part names, it validates that the database name matches the current database, since PostgreSQL does not support cross-database references. The function sets the output parameters to point to the extracted schema and object names, with the schema name set to NULL for unqualified names.

## Parameters / Member Variables
- `names`: A List of String nodes representing the qualified name components
- `nspname_p`: Output parameter for schema name (set to NULL if no explicit schema)  
- `objname_p`: Output parameter for object name

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - strVal
  - linitial
  - lsecond
  - lthird
  - [get_database_name](../g/get_database_name.md)
  - [NameListToString](../N/NameListToString.md)
  - ereport
- Called from (representative examples):
  - [FuncnameGetCandidates](../F/FuncnameGetCandidates.md)
  - [OpernameGetOprid](../O/OpernameGetOprid.md)
  - [OpernameGetCandidates](../O/OpernameGetCandidates.md)
  - [get_statistics_object_oid](../g/get_statistics_object_oid.md)
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md)

## Notes and Other Information
- Supports 1-part (object), 2-part (schema.object), and 3-part (database.schema.object) qualified names
- For 3-part names, validates the database component matches the current database
- Cross-database references are explicitly not supported and will raise an error
- Too many name components (more than 3) will result in a syntax error
- The function modifies the output parameters rather than returning values
- Widely used throughout the PostgreSQL codebase for parsing qualified object names
- Located in src/backend/catalog/namespace.c:3301-3354

## Simplified Source

```c
void
DeconstructQualifiedName(const List *names,
                        char **nspname_p,
                        char **objname_p)
{
    char *catalogname;
    char *schemaname = NULL;
    char *objname = NULL;

    switch (list_length(names)) {
        case 1:
            // Unqualified name: "object"
            objname = strVal(linitial(names));
            break;

        case 2:
            // Schema-qualified name: "schema.object"
            schemaname = strVal(linitial(names));
            objname = strVal(lsecond(names));
            break;

        case 3:
            // Fully-qualified name: "database.schema.object"
            catalogname = strVal(linitial(names));
            schemaname = strVal(lsecond(names));
            objname = strVal(lthird(names));

            // Validate catalog name matches current database
            if (strcmp(catalogname, get_database_name(MyDatabaseId)) != 0) {
                ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cross-database references are not implemented: %s",
                               NameListToString(names))));
            }
            break;

        default:
            // Too many components
            ereport(ERROR,
                   (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("improper qualified name (too many dotted names): %s",
                           NameListToString(names))));
            break;
    }

    *nspname_p = schemaname;
    *objname_p = objname;
}
```