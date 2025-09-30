# QualifiedNameGetCreationNamespace

## Location
[src/backend/catalog/namespace.c:3487-3534](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3487-L3534)

## Overview
Parses a possibly-qualified object name and determines the appropriate namespace for object creation, extracting the object name and handling default schema resolution.

## Definition

```c
struct the name list */
	DeconstructQualifiedName(names, &schemaname, objname_p);
```
## Detailed Description
This function is central to PostgreSQL's object creation process, handling the parsing and resolution of qualified names (schema.object) to determine where new database objects should be created. It supports both fully-qualified names (with explicit schema) and unqualified names (using default creation namespace).

The function performs several key operations: it deconstructs the input name list to separate schema and object names, handles the special pg_temp alias for temporary schemas, resolves explicit schema names to OIDs, and falls back to the default creation namespace when no schema is specified. It includes logic to initialize temporary namespaces when needed and ensures a valid creation target exists.

Unlike permission-checking variants, this function focuses purely on namespace resolution and does not validate CREATE rights - callers must perform appropriate permission checks separately.

## Parameters / Member Variables
- `names`: List of strings representing a potentially qualified object name (e.g., ["schema", "object"] or ["object"])
- `objname_p`: Output parameter that receives the extracted object name (last component of the name list)

## Dependencies
- Functions called/Symbols referenced:
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md) (to parse the qualified name)
  - [AccessTempTableNamespace](../A/AccessTempTableNamespace.md) (for temp namespace initialization)  
  - [get_namespace_oid](../g/get_namespace_oid.md) (to resolve schema names to OIDs)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (to refresh namespace search path)
  - ereport/ERROR (for error reporting)
- Called from (representative examples):
  - [DefineAggregate](../D/DefineAggregate.md)
  - [DefineCollation](../D/DefineCollation.md)
  - [CreateFunction](../C/CreateFunction.md)
  - [DefineOperator](../D/DefineOperator.md)
  - [DefineType](../D/DefineType.md)
  - [DefineDomain](../D/DefineDomain.md)
  - [CreateStatistics](../C/CreateStatistics.md)

## Notes and Other Information
- Does not perform permission checks - callers must validate CREATE rights separately
- May trigger CommandCounterIncrement operations when temp namespace initialization is required
- Handles both qualified (schema.object) and unqualified (object) names appropriately
- Falls back to activeCreationNamespace when no explicit schema is provided
- Throws an error if no valid creation namespace can be determined
- Essential component of PostgreSQL's DDL command processing infrastructure
- Returns the target namespace OID and extracts the object name via output parameter

## Simplified Source
```c
Oid
QualifiedNameGetCreationNamespace(const List *names, char **objname_p)
{
    char *schemaname;
    Oid namespaceId;

    // Parse the qualified name into schema and object components
    DeconstructQualifiedName(names, &schemaname, objname_p);

    if (schemaname) {
        // Handle special pg_temp alias
        if (strcmp(schemaname, "pg_temp") == 0) {
            AccessTempTableNamespace(false);
            return myTempNamespace;
        }
        // Use explicitly specified schema
        namespaceId = get_namespace_oid(schemaname, false);
    }
    else {
        // Use default creation namespace
        recomputeNamespacePath();
        if (activeTempCreationPending) {
            AccessTempTableNamespace(true);
            return myTempNamespace;
        }
        namespaceId = activeCreationNamespace;
        if (!OidIsValid(namespaceId))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                           errmsg("no schema has been selected to create in")));
    }

    return namespaceId;
}
```