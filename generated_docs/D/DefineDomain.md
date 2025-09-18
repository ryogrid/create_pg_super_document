# DefineDomain

## Location
[src/backend/commands/typecmds.c:697-1146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L697-L1146)

## Overview
DefineDomain creates a new domain type, which is a specialized type that inherits properties from a base type but can have additional constraints, default values, and NOT NULL specifications applied.

## Definition


## Detailed Description
DefineDomain implements PostgreSQL's CREATE DOMAIN command by creating a new domain type that acts as a constrained version of an existing base type. Domains allow users to define commonly-used data types with specific constraints that are automatically applied wherever the domain is used.

The function performs several key operations:
1. **Base Type Validation**: Ensures the base type is valid and supports domain creation (base types, composite types, other domains, enums, ranges, and multiranges are supported, but pseudotypes are not)
2. **Property Inheritance**: Inherits most properties from the base type including I/O functions, alignment, storage, and physical characteristics
3. **Constraint Processing**: Handles DEFAULT, NOT NULL, NULL, and CHECK constraints specific to the domain
4. **Array Type Creation**: Automatically creates a corresponding array type for the domain
5. **Collation Management**: Handles collation inheritance and explicit collation specification

The function uses specialized domain I/O functions (domain_in, domain_recv) that perform constraint checking during input operations, while output functions are inherited from the base type.

## Parameters / Member Variables
- : CreateDomainStmt structure containing all domain definition information
  - localdomain: Qualified name list for the new domain
  - : TypeName structure identifying the base type
  - : List of constraints to apply to the domain
  - : Optional collation specification

### Key Constraint Types Supported:
- : Provides default value for the domain
- : Makes the domain non-nullable
- : Explicitly allows NULL (overrides base type)
- : Adds check constraints for value validation

## Dependencies
- Functions called/Symbols referenced:
  - [TypeCreate](../T/TypeCreate.md): Creates the actual domain and array type entries
  - [typenameType](../t/typenameType.md): Resolves the base type name to a type tuple
  - [moveArrayTypeName](../m/moveArrayTypeName.md): Handles array type name conflicts
  - [makeArrayTypeName](../m/makeArrayTypeName.md): Generates array type names
  - [AssignTypeArrayOid](../A/AssignTypeArrayOid.md): Allocates OID for the array type
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md): Parses qualified names
  - [cookDefault](../c/cookDefault.md): Processes default value expressions
  - [domainAddCheckConstraint](../d/domainAddCheckConstraint.md): Adds domain-specific check constraints
  - [domainAddNotNullConstraint](../d/domainAddNotNullConstraint.md): Adds domain-specific NOT NULL constraints
  - [get_collation_oid](../g/get_collation_oid.md): Resolves collation names

- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main DDL command processing

## Notes and Other Information
- Domains inherit most physical properties from their base type but can override logical constraints
- Domain I/O functions (F_DOMAIN_IN, F_DOMAIN_RECV) perform constraint validation during input
- Domains cannot have type modifiers (typmodin/typmodout are always InvalidOid)
- Domains don't support subscripting directly (the parser reduces to base type before subscripting)
- The function automatically creates array types for domains, following the same pattern as base types
- Check constraints are processed after domain creation since they need the domain's OID
- Supports inheritance of default values from base type, with ability to override
- Collation must be compatible with the base type's collation capabilities
- Domain array types use standard array I/O functions but inherit the domain's constraints through the element type