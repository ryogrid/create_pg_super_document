CheckAttributeType

## Overview
CheckAttributeType verifies that a proposed attribute data type is valid for use in PostgreSQL tables, performing recursive validation for complex types and enforcing restrictions on pseudo-types.

## Definition
void CheckAttributeType(const char *attname, Oid atttypid, Oid attcollation, List *containing_rowtypes, int flags)

## Detailed Description
CheckAttributeType is a comprehensive type validation function that ensures data types are suitable for table columns. It performs different validation strategies based on the type category: for pseudo-types, it enforces restrictions while allowing certain exceptions based on flags; for domain types, it recursively validates the base type; for composite types, it validates all component attributes while preventing infinite recursion; for range types, it validates the subtype; and for array types, it validates the element type.

The function includes sophisticated recursive containment detection to prevent self-referential composite types that would create infinite recursion. It also validates that collatable types have proper collation information. The validation is designed to catch type definition errors early in the relation creation process.

## Parameters / Member Variables
- attname: The name of the attribute being validated (used in error messages)
- atttypid: The OID of the data type to validate
- attcollation: The collation OID for the attribute (required for collatable types)
- containing_rowtypes: List of rowtype OIDs to detect recursive containment (pass NIL for new rowtypes)
- flags: Bitmask controlling validation behavior (CHKATYPE_ANYARRAY, CHKATYPE_ANYRECORD, CHKATYPE_IS_PARTKEY)

## Dependencies
- Functions called/Symbols referenced:
  - get_typtype (determines type category)
  - check_stack_depth (prevents stack overflow in recursion)
  - getBaseType (gets base type for domains)
  - get_typ_typrelid (gets relation OID for composite types)
  - relation_open, relation_close (access composite type definitions)
  - get_range_subtype, get_range_collation (range type introspection)
  - get_element_type (array element type introspection)
  - type_is_collatable (determines if type requires collation)
  - list_member_oid, lappend_oid, list_delete_last (list manipulation for recursion tracking)
- Called from (representative examples):
  - CheckAttributeNamesTypes (in src/backend/catalog/heap.c:512)
  - CheckAttributeType (recursive calls at multiple lines)
  - ConstructTupleDescriptor (in src/backend/catalog/index.c:410)
  - ATExecAddColumn (in src/backend/commands/tablecmds.c:7177)

## Notes and Other Information
- Recursively validates complex types including composites, domains, ranges, and arrays
- Prevents infinite recursion in composite types through containing_rowtypes tracking
- Allows certain pseudo-types (ANYARRAY, RECORD, RECORDARRAY) based on flags
- Provides different error message phrasing for partition key columns vs regular columns
- Enforces collation requirements for collatable types
- Stack depth checking prevents stack overflow during deep recursion
- Critical for maintaining type system integrity in PostgreSQL
- Located in src/backend/catalog/heap.c:549-681