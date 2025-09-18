# btadjustmembers

## Location
[src/backend/access/nbtree/nbtvalidate.c:293-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtvalidate.c#L293-L380)

## Overview
Pre-processing function that adjusts dependency relationships when adding operators and functions to a btree operator family, determining whether each member should be tied to a specific operator class or the operator family.

## Definition


## Detailed Description
The  function manages the dependency relationships between btree operators/functions and their containing operator classes or families. It implements a sophisticated dependency management strategy:

1. **Cross-type Operations**: Operations involving different left and right operand types are always assigned as "loose" members of the operator family rather than being tied to a specific operator class.

2. **Optional Support Functions**: Support functions other than the mandatory comparison function (BTORDER_PROC) are treated as loose family members since they are optional.

3. **Same-type Operations**: For operations where left and right types match, the function attempts to bind them to an appropriate operator class. If no suitable operator class exists, it falls back to a loose family dependency.

4. **Dependency Types**: The function sets two key flags for each operation:
   - : Determines if the dependency prevents dropping the referenced object
   - : Indicates whether the reference is to the family (true) or an operator class (false)

This approach optimizes dependency management while avoiding the creation of incomplete operator classes, which could cause system inconsistencies.

## Parameters / Member Variables
- : The OID of the btree operator family being modified
- : The OID of the operator class context (may be InvalidOid)
- : List of operators to be added to the family
- : List of support functions to be added to the family

## Dependencies
- Functions called/Symbols referenced:
  - CommandCounterIncrement
  - [get_opclass_input_type](../g/get_opclass_input_type.md)
  - [list_concat_copy](../l/list_concat_copy.md)
  - [opclass_for_family_datatype](../o/opclass_for_family_datatype.md)
  - BTORDER_PROC (constant)
- Called from:
  - [bthandler](bthandler.md) (in btree access method handler)

## Notes and Other Information
- The function includes cache optimization by remembering the most recently used operator class input type to avoid repeated expensive lookups
- Creates a potential dump/reload hazard where object restoration order could affect final dependency relationships
- During CREATE OPERATOR CLASS operations, CommandCounterIncrement() is called to ensure visibility of the new pg_opclass row
- The function processes operators and functions identically by concatenating their lists, reducing code duplication
- Automatically "fixes" incorrectly bound cross-type operators by making them loose family members rather than throwing errors
- The dependency strategy balances system consistency with operational flexibility, preferring specific operator class dependencies when available but gracefully falling back to family dependencies when necessary