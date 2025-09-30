# get_object_address_attribute

## Location
[src/backend/catalog/objectaddress.c:1494-1544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L1494-L1544)

## Overview
Finds the ObjectAddress for a specific attribute (column) within a relation, serving as a helper function for object addressing in PostgreSQL's object management system.

## Definition

```c
struct return value. */
	attnum = get_attnum(reloid, attname);
```
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
  - [list_copy_head](../l/list_copy_head.md) (copies all but last elements of list)
  - [relation_openrv](../r/relation_openrv.md) (opens relation by RangeVar)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md) (creates RangeVar from name list)
  - [get_attnum](get_attnum.md) (retrieves attribute number by name)
  - [NameListToString](../N/NameListToString.md) (converts name list to string for error messages)
  - [relation_close](../r/relation_close.md) (closes relation when error occurs)
- Called from (representative examples):
  - [get_object_address](get_object_address.md) (main object address resolution function)

## Notes and Other Information
- The function requires at least 2 elements in the object list (relation name + column name)
- Returns ObjectAddress with classId set to RelationRelationId, objectId set to the relation's OID, and objectSubId set to the attribute number
- When missing_ok is true and the column doesn't exist, returns an ObjectAddress with InvalidOid and InvalidAttrNumber
- The opened relation is returned via the relp parameter and must be closed by the caller
- No support for missing_ok when the relation itself doesn't exist (marked with XXX comment)

## Simplified Source

```c
static ObjectAddress
get_object_address_attribute(ObjectType objtype, List *object,
                            Relation *relp, LOCKMODE lockmode,
                            bool missing_ok)
{
    ObjectAddress address;
    List *relname;
    Relation relation;
    const char *attname;
    AttrNumber attnum;

    // Validate input: need at least relation.column
    if (list_length(object) < 2)
        ereport(ERROR, "column name must be qualified");

    // Extract column name and relation name
    attname = strVal(llast(object));
    relname = list_copy_head(object, list_length(object) - 1);

    // Open the relation and get its OID
    relation = relation_openrv(makeRangeVarFromNameList(relname), lockmode);
    Oid reloid = RelationGetRelid(relation);

    // Look up the attribute number by name
    attnum = get_attnum(reloid, attname);

    if (attnum == InvalidAttrNumber) {
        // Column doesn't exist - handle based on missing_ok
        if (!missing_ok)
            ereport(ERROR, "column does not exist");

        // Return invalid address and close relation
        address.classId = RelationRelationId;
        address.objectId = InvalidOid;
        address.objectSubId = InvalidAttrNumber;
        relation_close(relation, lockmode);
        return address;
    }

    // Build valid ObjectAddress for the column
    address.classId = RelationRelationId;
    address.objectId = reloid;
    address.objectSubId = attnum;

    *relp = relation;  // Return opened relation to caller
    return address;
}
```