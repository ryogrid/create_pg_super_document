# check_publication_add_schema

## Location
[src/backend/catalog/pg_publication.c:98-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L98-L136)

## Overview
A static validation function that checks if a schema can be added to a publication, throwing appropriate errors if the schema is not suitable for publication.

## Definition
```c
static void check_publication_add_schema(Oid schemaid)
```

## Detailed Description
This function performs validation checks to ensure that a given schema can be safely added to a logical replication publication. It enforces two main restrictions:

1. **System Schema Restriction**: System schemas (catalog namespaces and toast namespaces) cannot be added to publications as they contain internal PostgreSQL metadata and structures that should not be replicated.

2. **Temporary Schema Restriction**: Temporary schemas cannot be added to publications because temporary objects are session-specific and not suitable for logical replication across different database instances.

The function uses PostgreSQL's error reporting mechanism to provide clear error messages when validation fails, including the schema name and specific reason for rejection.

## Parameters / Member Variables
- `schemaid`: An Oid (Object ID) representing the schema being validated for publication inclusion

## Dependencies
- Functions called/Symbols referenced:
  - [IsCatalogNamespace](../I/IsCatalogNamespace.md)
  - [IsToastNamespace](../I/IsToastNamespace.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [isAnyTempNamespace](../i/isAnyTempNamespace.md)
  - ereport (error reporting)
- Called from:
  - [publication_add_schema](../p/publication_add_schema.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_publication.c compilation unit
- The function only performs validation and does not modify any state
- Error messages include the schema name obtained via get_namespace_name() for user clarity
- The function follows the same error handling pattern as check_publication_add_relation
- Unlike table validation, schema validation is simpler and only checks namespace properties
- Location: src/backend/catalog/pg_publication.c:98-136

## Simplified Source

```c
static void
check_publication_add_schema(Oid schemaid)
{
    // System namespaces (catalog and toast) cannot be published
    if (IsCatalogNamespace(schemaid) || IsToastNamespace(schemaid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add schema \"%s\" to publication",
                        get_namespace_name(schemaid)),
                 errdetail("This operation is not supported for system schemas.")));

    // Temporary namespaces cannot be published
    if (isAnyTempNamespace(schemaid))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot add schema \"%s\" to publication",
                        get_namespace_name(schemaid)),
                 errdetail("Temporary schemas cannot be replicated.")));
}
```