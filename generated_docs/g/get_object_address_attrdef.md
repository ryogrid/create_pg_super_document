# get_object_address_attrdef

## Location
[src/backend/catalog/objectaddress.c:1545-1602](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1545-L1602)

## Overview
Finds the ObjectAddress for an attribute's default value, resolving column default expressions in PostgreSQL's object management system.

## Definition
static ObjectAddress get_object_address_attrdef(ObjectType objtype, List *object, Relation *relp, LOCKMODE lockmode, bool missing_ok)

## Detailed Description
This static function resolves a reference to a column's default value into an ObjectAddress structure. It takes a qualified column name, opens the specified relation, looks up the attribute number, and then retrieves the OID of the corresponding pg_attrdef entry using GetAttrDefaultOid. The function constructs an ObjectAddress with AttrDefaultRelationId as classId and the default value's OID as objectId. It handles cases where the column exists but has no default value, either reporting an error or returning an invalid ObjectAddress based on the missing_ok parameter.

## Parameters / Member Variables
- : The type of object being addressed (not directly used in this function)
- : A List containing the qualified column name, where the last element is the column name and preceding elements form the relation name
- : Output parameter - pointer to store the opened Relation structure
- : The lock mode to acquire on the relation when opening it
- : If true, returns an invalid ObjectAddress when the column default doesn't exist instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - llast (extracts last element from list)
  - [list_copy_head](../l/list_copy_head.md) (copies all but last elements of list)
  - [relation_openrv](../r/relation_openrv.md) (opens relation by RangeVar)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (creates RangeVar from name list)
  - [get_attnum](get_attnum.md) (retrieves attribute number by name)
  - [GetAttrDefaultOid](../G/GetAttrDefaultOid.md) (retrieves OID of pg_attrdef entry for column default)
  - [NameListToString](../N/NameListToString.md) (converts name list to string for error messages)
  - [relation_close](../r/relation_close.md) (closes relation when error occurs)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution function)

## Notes and Other Information
- Requires at least 2 elements in the object list (relation name + column name)
- Returns ObjectAddress with classId set to AttrDefaultRelationId and objectSubId set to 0
- Checks if the relation has constraints (tupdesc->constr != NULL) before attempting to find the default
- When missing_ok is true and no default exists, returns an ObjectAddress with InvalidOid and InvalidAttrNumber
- The opened relation is returned via the relp parameter and must be closed by the caller
- No support for missing_ok when the relation itself doesn't exist (marked with XXX comment)
- Uses GetAttrDefaultOid helper function to retrieve the pg_attrdef OID for the specified column