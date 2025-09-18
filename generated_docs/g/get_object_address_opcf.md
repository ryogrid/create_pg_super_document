# get_object_address_opcf

## Location
src/backend/catalog/objectaddress.c: 1642 - 1679

## Overview
Finds the ObjectAddress for an operator class or operator family, resolving access method-qualified names into their corresponding catalog entries.

## Definition
static ObjectAddress get_object_address_opcf(ObjectType objtype, List *object, bool missing_ok)

## Detailed Description
This static function resolves operator class or operator family names into ObjectAddress structures. It first extracts and validates the access method name from the beginning of the object list, then uses either get_opclass_oid or get_opfamily_oid to find the specific operator class or family within that access method. The function supports both OBJECT_OPCLASS and OBJECT_OPFAMILY object types, setting the appropriate catalog relation ID and delegating the actual lookup to specialized functions that handle the missing_ok parameter.

## Parameters / Member Variables
- : The type of object being addressed - must be either OBJECT_OPCLASS or OBJECT_OPFAMILY
- : A List where the first element is the access method name and remaining elements specify the operator class/family name
- : If true, allows graceful handling when the operator class/family doesn't exist (passed through to lookup functions)

## Dependencies
- Functions called/Symbols referenced:
  - [get_index_am_oid](get_index_am_oid.md) (looks up access method OID and validates it's for indexes)
  - [list_copy_tail](../l/list_copy_tail.md) (creates list copy excluding first element)
  - [get_opclass_oid](get_opclass_oid.md) (retrieves operator class OID within specified access method)
  - [get_opfamily_oid](get_opfamily_oid.md) (retrieves operator family OID within specified access method)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution function)
  - [get_object_address_opf_member](get_object_address_opf_member.md) (for resolving operator family members)

## Notes and Other Information
- Expects the object list to have the access method name as the first element
- Returns ObjectAddress with objectSubId always set to 0
- For OBJECT_OPCLASS, sets classId to OperatorClassRelationId
- For OBJECT_OPFAMILY, sets classId to OperatorFamilyRelationId
- No direct missing_ok support for the access method lookup (marked with XXX comment)
- The missing_ok parameter is passed through to get_opclass_oid/get_opfamily_oid for the actual operator class/family lookup
- Uses elog(ERROR) for unexpected object types, which should not occur in normal operation