# get_tablespace_name

## Location
[src/backend/commands/tablespace.c:1472-1510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L1472-L1510)

## Overview
This function looks up the name of a tablespace given its OID (Object IDentifier), returning a palloc'd string containing the tablespace name or NULL if no such tablespace exists.

## Definition

```c
char *
get_tablespace_name(Oid spc_oid)
```
## Detailed Description
The  function performs a lookup in the  system catalog to retrieve the name associated with a given tablespace OID. The function uses a heap scan rather than an indexed lookup based on the assumption that  typically contains only a few entries, making a sequential scan more efficient than index access overhead.

The function opens the  relation with an AccessShareLock, performs a catalog scan with an equality condition on the OID column, and extracts the tablespace name from the matching tuple. The returned string is allocated using , making it the caller's responsibility to free the memory when no longer needed.

## Parameters / Member Variables
- : The OID of the tablespace whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - : Opens the pg_tablespace system catalog
  - : Initializes scan key for OID equality search
  - : Begins catalog scan
  - : Retrieves next tuple from scan
  - : Checks if tuple is valid
  - : Duplicates string in palloc'd memory
  - : Ends the catalog scan
  - : Closes the relation
  - : Type for table scan descriptor
  - : Scan direction constant
  - : Struct type for pg_tablespace tuples
- Called from (representative examples):
  - : Object description generation
  - : Object identity formatting
  - : Index creation with tablespace specification
  - : Table creation with tablespace specification
  - : Index definition reconstruction
  - : Tablespace size calculation

## Notes and Other Information
- Returns NULL if the specified tablespace OID does not exist
- The returned string is palloc'd and must be freed by the caller
- Uses heap scan instead of index scan for efficiency with small pg_tablespace catalog
- Assumes at most one matching tuple (enforces uniqueness constraint)
- Holds AccessShareLock on pg_tablespace during the operation
- Used extensively throughout the system for error messages, object descriptions, and DDL operations

## Simplified Source

```c
char *
get_tablespace_name(Oid spc_oid)
{
    char *result;
    Relation rel;
    TableScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];

    // Open pg_tablespace catalog for scanning
    rel = table_open(TableSpaceRelationId, AccessShareLock);

    // Set up scan key to search by OID
    ScanKeyInit(&entry[0], Anum_pg_tablespace_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(spc_oid));
    scandesc = table_beginscan_catalog(rel, 1, entry);
    tuple = heap_getnext(scandesc, ForwardScanDirection);

    // Extract tablespace name if found
    if (HeapTupleIsValid(tuple))
        result = pstrdup(NameStr(((Form_pg_tablespace) GETSTRUCT(tuple))->spcname));
    else
        result = NULL;

    table_endscan(scandesc);
    table_close(rel, AccessShareLock);

    return result;
}
```