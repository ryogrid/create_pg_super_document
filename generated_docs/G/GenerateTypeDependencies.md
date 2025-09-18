# GenerateTypeDependencies

## Location
src/backend/catalog/pg_type.c: 557 - 764

## Overview
GenerateTypeDependencies creates and manages the complete set of dependency relationships for a PostgreSQL type, handling dependencies on functions, namespaces, owners, base types, and other related objects.

## Definition


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
  - heap_getattr, stringToNode, TextDatumGetCString, DatumGetAclPCopy
  - deleteDependencyRecordsFor, deleteSharedDependencyRecordsFor
  - ObjectAddressSet
  - new_object_addresses, add_exact_object_address, free_object_addresses
  - record_object_address_dependencies
  - recordDependencyOnOwner, recordDependencyOnNewAcl
  - recordDependencyOnCurrentExtension
  - recordDependencyOn, recordDependencyOnExpr
- Called from (representative examples):
  - TypeShellMake (src/backend/catalog/pg_type.c:160)
  - TypeCreate (src/backend/catalog/pg_type.c:497)
  - AlterDomainDefault (src/backend/commands/typecmds.c:2676)
  - AlterTypeRecurse (src/backend/commands/typecmds.c:4625)

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