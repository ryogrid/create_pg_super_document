# GetAttrDefaultColumnAddress

## Location
src/backend/catalog/pg_attrdef.c: 387 - 416

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
  - table_open/table_close: Opens and closes pg_attrdef catalog with shared lock
  - ScanKeyInit: Initializes scan key for OID-based lookup
  - systable_beginscan: Begins scan using AttrDefaultOidIndexId for efficient OID lookup
  - systable_getnext: Retrieves matching tuple from scan
  - systable_endscan: Ends system table scan
  - GETSTRUCT: Extracts pg_attrdef structure from heap tuple
  - ObjectIdGetDatum: Converts OID to datum format for scanning

- Called from (representative examples):
  - getObjectDescription: During generation of human-readable object descriptions
  - getObjectIdentityParts: When constructing object identity information for DDL
  - RememberAllDependentForRebuilding: During table rebuild operations to track dependencies

## Notes and Other Information
The function uses shared locking throughout its operation, making it safe for concurrent access with other readers. It utilizes the AttrDefaultOidIndexId index for efficient OID-based lookups rather than scanning the entire table. The returned ObjectAddress follows PostgreSQL's standard addressing convention for table columns, where the class ID identifies the object type (RelationRelationId for relations), the object ID identifies the specific relation, and the object sub ID identifies the specific column within that relation. This function is commonly used in dependency tracking, object description generation, and various DDL operations where the system needs to identify which column a default expression belongs to. The InvalidObjectAddress return value provides a clear indication when the specified pg_attrdef OID does not exist in the catalog.