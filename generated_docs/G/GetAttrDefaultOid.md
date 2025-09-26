# GetAttrDefaultOid

## Location
[src/backend/catalog/pg_attrdef.c:345-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L345-L386)

## Overview
GetAttrDefaultOid retrieves the OID of the pg_attrdef entry for a specified column's default expression, returning InvalidOid if no default exists.

## Definition
```c
Oid GetAttrDefaultOid(Oid relid, AttrNumber attnum)
```

## Detailed Description
This function performs a lookup in the pg_attrdef system catalog to find the OID of the default expression entry for a specific column. It uses a systematic scan with a composite key consisting of both the relation OID and attribute number to locate the corresponding pg_attrdef tuple. The function uses shared locking (AccessShareLock) since it only reads data without making modifications. If a matching default entry is found, it extracts and returns the OID from the tuple; otherwise, it returns InvalidOid to indicate no default exists. This function provides a clean interface for other parts of the system to check for the existence of defaults and obtain their identifiers for further operations.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the column
- `attnum`: The attribute number (column number) to check for a default

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/table_close: Opens and closes pg_attrdef catalog with shared lock
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan keys for relation ID and attribute number
  - [systable_beginscan](../s/systable_beginscan.md): Begins scan using AttrDefaultIndexId for efficient lookup
  - [systable_getnext](../s/systable_getnext.md): Retrieves matching tuple from scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - GETSTRUCT: Extracts pg_attrdef structure from heap tuple
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)/Int16GetDatum: Converts values to datum format

- Called from (representative examples):
  - [get_object_address_attrdef](../g/get_object_address_attrdef.md): During object address resolution for defaults
  - [ATExecSetExpression](../A/ATExecSetExpression.md): When setting column expressions (checks existing defaults)  
  - [ATExecDropExpression](../A/ATExecDropExpression.md): When dropping column expressions
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): During column type changes to handle existing defaults

## Notes and Other Information
The function uses shared locking throughout the operation, making it safe for concurrent execution with other readers while ensuring consistency. It leverages the AttrDefaultIndexId index for efficient lookup performance rather than performing a full table scan. The function is designed as a simple query interface and does not perform any modifications to the catalog. The return of InvalidOid serves as a clear indication that no default exists, allowing callers to distinguish between existing defaults and missing defaults. This function is commonly used in DDL operations where the system needs to determine whether default handling is required for specific columns.