# get_object_address_attribute

## Location
src/backend/catalog/objectaddress.c: 1494 - 1544

## Overview
Finds the ObjectAddress for a specific attribute (column) within a relation, serving as a helper function for object addressing in PostgreSQL's object management system.

## Definition


## Detailed Description
This static function resolves a column reference into an ObjectAddress structure, which uniquely identifies database objects within PostgreSQL's catalog system. The function takes a qualified column name (relation.column), opens the specified relation, looks up the attribute number for the given column name, and constructs an ObjectAddress with the relation's OID as objectId and the attribute number as objectSubId. The function handles error cases gracefully when the column doesn't exist, either reporting an error or returning an invalid ObjectAddress based on the missing_ok parameter.

## Parameters / Member Variables
- : The type of object being addressed (though not directly used in this function)
- : A List containing the qualified column name, where the last element is the column name and preceding elements form the relation name
- : Output parameter - pointer to store the opened Relation structure
- : The lock mode to acquire on the relation when opening it
- : If true, returns an invalid ObjectAddress when the column doesn't exist instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - llast (extracts last element from list)
  - list_copy_head (copies all but last elements of list)
  - relation_openrv (opens relation by RangeVar)
  - makeRangeVarFromNameList (creates RangeVar from name list)
  - get_attnum (retrieves attribute number by name)
  - NameListToString (converts name list to string for error messages)
  - relation_close (closes relation when error occurs)
- Called from (representative examples):
  - get_object_address (main object address resolution function)

## Notes and Other Information
- The function requires at least 2 elements in the object list (relation name + column name)
- Returns ObjectAddress with classId set to RelationRelationId, objectId set to the relation's OID, and objectSubId set to the attribute number
- When missing_ok is true and the column doesn't exist, returns an ObjectAddress with InvalidOid and InvalidAttrNumber
- The opened relation is returned via the relp parameter and must be closed by the caller
- No support for missing_ok when the relation itself doesn't exist (marked with XXX comment)