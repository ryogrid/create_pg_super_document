# typeInheritsFrom

## Location
src/backend/catalog/pg_inherits.c: 406 - 507

## Overview
Determines whether one type inherits from another by checking the inheritance hierarchy in the PostgreSQL catalog system.

## Definition


## Detailed Description
This function determines whether the first type (subclassTypeId) is a complex type (class type) that inherits from the second type (superclassTypeId). The function essentially asks whether the first type is guaranteed to be coercible to the second type.

The function performs a breadth-first traversal of the inheritance graph starting from the subclass relation up to find if it inherits from the superclass. It allows the first type to be a domain over a complex type that inherits from the second, but the second type cannot be a domain.

The algorithm:
1. Converts both type OIDs to their associated relation OIDs
2. Checks if the superclass has any subclasses (optimization)
3. Performs breadth-first search through the pg_inherits catalog
4. Maintains visited list to avoid cycles and duplicate work
5. Returns true if inheritance relationship is found

## Parameters / Member Variables
- : OID of the type that may inherit from the superclass
- : OID of the potential superclass type

## Dependencies
- Functions called/Symbols referenced:
  - [typeOrDomainTypeRelid](typeOrDomainTypeRelid.md): Converts type OID to relation OID, handling domains
  - [typeidTypeRelid](typeidTypeRelid.md): Converts type OID to relation OID for complex types only
  - [has_subclass](../h/has_subclass.md): Checks if a relation has any child relations
  - list_make1_oid: Creates a list with one OID element
  - [list_member_oid](../l/list_member_oid.md): Checks if OID is already in the list
  - lappend_oid: Appends OID to list
  - [systable_beginscan](../s/systable_beginscan.md): Begins system table scan
  - [systable_getnext](../s/systable_getnext.md): Gets next tuple from system scan
  - [list_free](../l/list_free.md): Frees memory allocated for lists
- Called from (representative examples):
  - [coerce_type](../c/coerce_type.md): Type coercion logic in parser
  - [can_coerce_type](../c/can_coerce_type.md): Type coercion checking in parser

## Notes and Other Information
- Uses breadth-first search to efficiently traverse inheritance hierarchies
- Handles multiple inheritance scenarios correctly
- Protects against infinite loops in case of cycles in pg_inherits
- The function is primarily used in type coercion logic to determine if one type can be safely converted to another
- Only works with complex types (class types), not scalar types
- Location: src/backend/catalog/pg_inherits.c:406-507