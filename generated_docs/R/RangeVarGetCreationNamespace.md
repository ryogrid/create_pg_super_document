# RangeVarGetCreationNamespace

## Location
[src/backend/catalog/namespace.c:654-738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L654-L738)

## Overview
A function that determines the appropriate namespace (schema) for creating a new relation based on a RangeVar specification, handling temporary tables, schema resolution, and default namespace selection.

## Definition
```c
Oid RangeVarGetCreationNamespace(const RangeVar *newRelation)
```

## Detailed Description
RangeVarGetCreationNamespace is responsible for selecting the correct namespace where a new relation should be created. It handles various scenarios including explicitly specified schemas, temporary table creation, and default namespace selection.

The function performs cross-database reference validation, processes the special 'pg_temp' schema alias, and manages temporary namespace initialization. When no explicit schema is specified, it uses the active creation namespace from the current search path.

Key behaviors include:
- Validation of cross-database references (currently not supported)
- Special handling of 'pg_temp' schema alias for temporary tables
- Automatic temporary namespace initialization when needed
- Default namespace selection from the search path
- Deferred permission checking (handled by callers)

## Parameters / Member Variables
- `newRelation`: RangeVar structure describing the relation to be created, containing optional catalog name, schema name, relation name, and persistence information

## Dependencies
- Functions called/Symbols referenced:
  - [get_database_name](../g/get_database_name.md)
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [AccessTempTableNamespace](../A/AccessTempTableNamespace.md)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
- Called from (representative examples):
  - [RangeVarGetAndCheckCreationNamespace](RangeVarGetAndCheckCreationNamespace.md)
  - [CreateTableAsRelExists](../C/CreateTableAsRelExists.md)
  - [generateSerialExtraStmts](../g/generateSerialExtraStmts.md)

## Notes and Other Information
- May trigger a CommandCounterIncrement operation during temporary namespace initialization
- Does not check USAGE rights on the target namespace (permission checking is delegated to callers)
- Returns InvalidOid and raises an error if no schema has been selected for creation
- The activeTempCreationPending flag indicates when temporary namespace initialization is needed
- Cross-database references are explicitly not supported and will raise an error

## Simplified Source

```c
Oid RangeVarGetCreationNamespace(const RangeVar *newRelation) {
    Oid namespaceId;

    // Check and reject cross-database references
    if (newRelation->catalogname) {
        if (strcmp(newRelation->catalogname, get_database_name(MyDatabaseId)) != 0) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cross-database references are not implemented")));
        }
    }

    if (newRelation->schemaname) {
        // Handle special pg_temp alias
        if (strcmp(newRelation->schemaname, "pg_temp") == 0) {
            AccessTempTableNamespace(false);
            return myTempNamespace;
        }
        // Use explicitly specified schema
        namespaceId = get_namespace_oid(newRelation->schemaname, false);
    } else if (newRelation->relpersistence == RELPERSISTENCE_TEMP) {
        // Temporary relation without explicit schema - use temp namespace
        AccessTempTableNamespace(false);
        return myTempNamespace;
    } else {
        // Use default creation namespace from search path
        recomputeNamespacePath();
        if (activeTempCreationPending) {
            AccessTempTableNamespace(true);
            return myTempNamespace;
        }
        namespaceId = activeCreationNamespace;
        if (!OidIsValid(namespaceId)) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                           errmsg("no schema has been selected to create in")));
        }
    }

    return namespaceId;
}
```