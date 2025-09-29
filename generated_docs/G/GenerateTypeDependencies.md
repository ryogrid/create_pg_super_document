# GenerateTypeDependencies

## Location
[src/backend/catalog/pg_type.c:557-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_type.c#L557-L764)

## Overview
GenerateTypeDependencies creates and manages the complete set of dependency relationships for a PostgreSQL type, handling dependencies on functions, namespaces, owners, base types, and other related objects.

## Definition

```c
void
GenerateTypeDependencies(HeapTuple typeTuple,
						 Relation typeCatalog,
						 Node *defaultExpr,
						 void *typacl,
						 char relationKind, /* only for relation rowtypes */
						 bool isImplicitArray,
						 bool isDependentType,
						 bool makeExtensionDep,
						 bool rebuild)
```
## Detailed Description
GenerateTypeDependencies is the comprehensive function responsible for establishing all dependency relationships for a PostgreSQL type. It analyzes the type definition and creates dependencies on various database objects including I/O functions, support functions, namespaces, owners, base types, collations, and default expressions.

The function operates in different modes based on the type characteristics. For dependent types (implicit arrays, multirange types, or relation rowtypes), it skips certain dependencies that are inherited through the parent object. For independent types, it creates direct dependencies on namespace and owner. The function handles extension membership, ACL dependencies, and supports both initial creation and rebuilding scenarios.

When rebuilding dependencies (for ALTER TYPE operations or shell type completion), it first removes existing dependencies before establishing new ones, except for extension dependencies which are preserved. The function optimizes bulk dependency recording using ObjectAddresses collections and handles special cases like composite types where dependency direction is reversed.

## Parameters
- : HeapTuple containing the pg_type row for the type
- : Open relation for the pg_type catalog
- : Parse tree for default value expression (NULL if not available)
- : Access control list for the type (NULL if not available)
- : Kind of associated relation for composite types
- : Whether this is an implicitly created array type
- : Whether this is a dependent type (array, multirange, relation rowtype)
- : Whether to create extension membership dependency
- : Whether to rebuild dependencies from scratch (removes existing first)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_getattr](../h/heap_getattr.md), stringToNode, TextDatumGetCString, DatumGetAclPCopy
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md), deleteSharedDependencyRecordsFor
  - ObjectAddressSet
  - [new_object_addresses](../n/new_object_addresses.md), add_exact_object_address, free_object_addresses
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md), recordDependencyOnNewAcl
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md)
  - [recordDependencyOn](../r/recordDependencyOn.md), recordDependencyOnExpr
- Called from (representative examples):
  - [TypeShellMake](../T/TypeShellMake.md) (src/backend/catalog/pg_type.c:160)
  - [TypeCreate](../T/TypeCreate.md) (src/backend/catalog/pg_type.c:497)
  - [AlterDomainDefault](../A/AlterDomainDefault.md) (src/backend/commands/typecmds.c:2676)
  - [AlterTypeRecurse](../A/AlterTypeRecurse.md) (src/backend/commands/typecmds.c:4625)

## Notes and Other Information
- Extracts default expression and ACL from tuple if not provided by caller for efficiency
- Uses bulk dependency recording via ObjectAddresses for better performance
- Handles dependent types specially - they inherit namespace/owner dependencies through parent objects
- Exception: multirange types need their own namespace dependency regardless of dependent status
- For composite types, creates reverse internal dependency (relation depends on type)
- For implicit array types, creates internal dependency on element type
- For other array types, creates normal dependency on element type
- Skips dependency on default collation (pinned object)
- Supports extension membership recording while preserving existing extension relationships during rebuild
- Does not handle multirange-to-range dependencies (handled by RangeCreate instead)
- Critical for maintaining referential integrity in PostgreSQL's type system

## Simplified Source

```c
void
GenerateTypeDependencies(HeapTuple typeTuple, Relation typeCatalog,
                        Node *defaultExpr, void *typacl, char relationKind,
                        bool isImplicitArray, bool isDependentType,
                        bool makeExtensionDep, bool rebuild)
{
    Form_pg_type typeForm = (Form_pg_type) GETSTRUCT(typeTuple);
    Oid typeObjectId = typeForm->oid;
    ObjectAddress myself, referenced;
    ObjectAddresses *addrs_normal;

    // Extract default expression and ACL if not provided
    if (defaultExpr == NULL)
    {
        Datum datum;
        bool isNull;
        datum = heap_getattr(typeTuple, Anum_pg_type_typdefaultbin,
                            RelationGetDescr(typeCatalog), &isNull);
        if (!isNull)
            defaultExpr = stringToNode(TextDatumGetCString(datum));
    }

    if (typacl == NULL)
    {
        Datum datum;
        bool isNull;
        datum = heap_getattr(typeTuple, Anum_pg_type_typacl,
                            RelationGetDescr(typeCatalog), &isNull);
        if (!isNull)
            typacl = DatumGetAclPCopy(datum);
    }

    // Remove existing dependencies if rebuilding
    if (rebuild)
    {
        deleteDependencyRecordsFor(TypeRelationId, typeObjectId, true);
        deleteSharedDependencyRecordsFor(TypeRelationId, typeObjectId, 0);
    }

    ObjectAddressSet(myself, TypeRelationId, typeObjectId);
    addrs_normal = new_object_addresses();

    // Create namespace dependency for non-dependent types or multiranges
    if (!isDependentType || typeForm->typtype == TYPTYPE_MULTIRANGE)
    {
        ObjectAddressSet(referenced, NamespaceRelationId, typeForm->typnamespace);
        add_exact_object_address(&referenced, addrs_normal);
    }

    // Create owner and ACL dependencies for non-dependent types
    if (!isDependentType)
    {
        recordDependencyOnOwner(TypeRelationId, typeObjectId, typeForm->typowner);
        recordDependencyOnNewAcl(TypeRelationId, typeObjectId, 0,
                                typeForm->typowner, typacl);
    }

    // Create extension dependency if requested
    if (makeExtensionDep)
        recordDependencyOnCurrentExtension(&myself, rebuild);

    // Add dependencies on I/O and support functions
    if (OidIsValid(typeForm->typinput))
    {
        ObjectAddressSet(referenced, ProcedureRelationId, typeForm->typinput);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if (OidIsValid(typeForm->typoutput))
    {
        ObjectAddressSet(referenced, ProcedureRelationId, typeForm->typoutput);
        add_exact_object_address(&referenced, addrs_normal);
    }

    // Add dependencies on other support functions
    if (OidIsValid(typeForm->typreceive))
    {
        ObjectAddressSet(referenced, ProcedureRelationId, typeForm->typreceive);
        add_exact_object_address(&referenced, addrs_normal);
    }

    if (OidIsValid(typeForm->typsend))
    {
        ObjectAddressSet(referenced, ProcedureRelationId, typeForm->typsend);
        add_exact_object_address(&referenced, addrs_normal);
    }

    // Add dependency on base type for domains
    if (OidIsValid(typeForm->typbasetype))
    {
        ObjectAddressSet(referenced, TypeRelationId, typeForm->typbasetype);
        add_exact_object_address(&referenced, addrs_normal);
    }

    // Add dependency on collation (skip default collation)
    if (OidIsValid(typeForm->typcollation) &&
        typeForm->typcollation != DEFAULT_COLLATION_OID)
    {
        ObjectAddressSet(referenced, CollationRelationId, typeForm->typcollation);
        add_exact_object_address(&referenced, addrs_normal);
    }

    // Record all normal dependencies in bulk
    record_object_address_dependencies(&myself, addrs_normal, DEPENDENCY_NORMAL);
    free_object_addresses(addrs_normal);

    // Record dependency on default expression
    if (defaultExpr)
        recordDependencyOnExpr(&myself, defaultExpr, NIL, DEPENDENCY_NORMAL);

    // Handle relation rowtype dependencies
    if (OidIsValid(typeForm->typrelid))
    {
        ObjectAddressSet(referenced, RelationRelationId, typeForm->typrelid);
        if (relationKind != RELKIND_COMPOSITE_TYPE)
            recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
        else
            recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
    }

    // Handle array type dependencies
    if (OidIsValid(typeForm->typelem))
    {
        ObjectAddressSet(referenced, TypeRelationId, typeForm->typelem);
        recordDependencyOn(&myself, &referenced,
                          isImplicitArray ? DEPENDENCY_INTERNAL : DEPENDENCY_NORMAL);
    }
}
```