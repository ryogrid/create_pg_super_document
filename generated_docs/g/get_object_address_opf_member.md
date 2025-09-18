# get_object_address_opf_member

## Location
[src/backend/catalog/objectaddress.c:1680-1791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1680-L1791)

## Overview
Finds the ObjectAddress for an operator family member (operator or support procedure), resolving complex specifications into pg_amop or pg_amproc catalog entries.

## Definition
static ObjectAddress get_object_address_opf_member(ObjectType objtype, List *object, bool missing_ok)

## Detailed Description
This static function resolves references to specific members within operator families - either operators (OBJECT_AMOP) or support procedures (OBJECT_AMPROC). It extracts the strategy/procedure number, resolves the operator family using get_object_address_opcf, processes left and right operand type specifications, and then searches the appropriate system catalog (pg_amop or pg_amproc) using a 4-way cache lookup. The function handles complex object specifications that include the operator family name, operand types, and member numbers, providing detailed error messages when members don't exist.

## Parameters / Member Variables
- : The type of object being addressed - must be either OBJECT_AMOP (operator) or OBJECT_AMPROC (support procedure)
- : A complex List structure where the first element contains the operator family specification (with strategy/procedure number as the last component) and the second element contains the operand type specifications
- : If true, allows graceful handling when the operator family member doesn't exist (passed through to type resolution and used for error handling)

## Dependencies
- Functions called/Symbols referenced:
  - llast (extracts last element from list)
  - [list_copy_head](../l/list_copy_head.md) (creates list copy excluding last element)
  - [get_object_address_opcf](get_object_address_opcf.md) (resolves operator family address)
  - lsecond (extracts second element from list)
  - [get_object_address_type](get_object_address_type.md) (resolves operand type addresses)
  - ObjectAddressSet (sets ObjectAddress fields)
  - [SearchSysCache4](../S/SearchSysCache4.md) (performs 4-way system catalog lookup)
  - [TypeNameToString](../T/TypeNameToString.md) (converts TypeName to string for error messages)
  - [getObjectDescription](getObjectDescription.md) (generates object description for error messages)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache entries)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution function)

## Notes and Other Information
- Handles complex nested object specifications with operator families, types, and member numbers
- For OBJECT_AMOP, searches pg_amop using AMOPSTRATEGY cache and sets classId to AccessMethodOperatorRelationId
- For OBJECT_AMPROC, searches pg_amproc using AMPROCNUM cache and sets classId to AccessMethodProcedureRelationId
- Supports up to 2 operand types (left and right), processing them from the second element of the object list
- No direct missing_ok support for the operator family lookup (delegates to get_object_address_opcf)
- Provides detailed error messages including member numbers, type names, and operator family descriptions
- Uses system cache lookups with operator family OID, left type OID, right type OID, and strategy/procedure number as keys
- Returns ObjectAddress with objectSubId set to 0 and objectId set to the pg_amop/pg_amproc entry's OID