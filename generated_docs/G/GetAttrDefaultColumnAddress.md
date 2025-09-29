# GetAttrDefaultColumnAddress

## Location
[src/backend/catalog/pg_attrdef.c:387-416](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L387-L416)

## Overview
GetAttrDefaultColumnAddress retrieves the relation OID and column number of the owning column for a given pg_attrdef entry, returning the information as an ObjectAddress structure.

## Definition
```c
ObjectAddress GetAttrDefaultColumnAddress(Oid attrdefoid)
```

## Detailed Description
This function performs a reverse lookup in the pg_attrdef system catalog, taking a pg_attrdef OID as input and returning the ObjectAddress of the column that owns that default expression. It searches the pg_attrdef table using the provided OID to locate the corresponding tuple, then extracts the relation ID (adrelid) and attribute number (adnum) from the tuple to construct an ObjectAddress. The returned ObjectAddress uses RelationRelationId as the class ID, the relation OID as the object ID, and the attribute number as the object sub ID, following PostgreSQL's standard object addressing scheme for columns. If no matching pg_attrdef entry is found, the function returns InvalidObjectAddress.

## Parameters / Member Variables  
- `attrdefoid`: The OID of the pg_attrdef entry whose owning column information is needed

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close: Opens and closes pg_attrdef catalog with shared lock
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for OID-based lookup
  - [systable_beginscan](../s/systable_beginscan.md): Begins scan using AttrDefaultOidIndexId for efficient OID lookup
  - [systable_getnext](../s/systable_getnext.md): Retrieves matching tuple from scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - GETSTRUCT: Extracts pg_attrdef structure from heap tuple
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md): Converts OID to datum format for scanning

- Called from (representative examples):
  - [getObjectDescription](../g/getObjectDescription.md): During generation of human-readable object descriptions
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md): When constructing object identity information for DDL
  - [RememberAllDependentForRebuilding](../R/RememberAllDependentForRebuilding.md): During table rebuild operations to track dependencies

## Notes and Other Information
The function uses shared locking throughout its operation, making it safe for concurrent access with other readers. It utilizes the AttrDefaultOidIndexId index for efficient OID-based lookups rather than scanning the entire table. The returned ObjectAddress follows PostgreSQL's standard addressing convention for table columns, where the class ID identifies the object type (RelationRelationId for relations), the object ID identifies the specific relation, and the object sub ID identifies the specific column within that relation. This function is commonly used in dependency tracking, object description generation, and various DDL operations where the system needs to identify which column a default expression belongs to. The InvalidObjectAddress return value provides a clear indication when the specified pg_attrdef OID does not exist in the catalog.

## Simplified Source

```c
ObjectAddress
GetAttrDefaultColumnAddress(Oid attrdefoid)
{
    ObjectAddress result = InvalidObjectAddress;
    Relation attrdef;
    ScanKeyData skey[1];
    SysScanDesc scan;
    HeapTuple tup;

    // Open pg_attrdef catalog
    attrdef = table_open(AttrDefaultRelationId, AccessShareLock);

    // Set up scan key for OID lookup
    ScanKeyInit(&skey[0], Anum_pg_attrdef_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(attrdefoid));
    scan = systable_beginscan(attrdef, AttrDefaultOidIndexId, true, NULL, 1, skey);

    // Get the matching tuple and extract column address
    if (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_attrdef atdform = (Form_pg_attrdef) GETSTRUCT(tup);

        result.classId = RelationRelationId;
        result.objectId = atdform->adrelid;
        result.objectSubId = atdform->adnum;
    }

    systable_endscan(scan);
    table_close(attrdef, AccessShareLock);

    return result;
}
```