# replorigin_by_oid

## Location
[src/backend/replication/logical/origin.c:465-505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L465-L505)

## Overview
Looks up a replication origin by its internal OID and returns the external name, providing reverse mapping from origin ID to origin name.

## Definition
```c
bool replorigin_by_oid(RepOriginId roident, bool missing_ok, char **roname)
```

## Detailed Description
This function performs a reverse lookup to find a replication origin's external name given its internal OID. It searches the system catalog (pg_replication_origin) using the system cache for efficient access. The function validates the input OID to ensure it's valid and not one of the special reserved values (InvalidRepOriginId or DoNotReplicateId).

When a matching origin is found, the function extracts the name from the catalog tuple, converts it from PostgreSQL's internal text format to a C string, and allocates memory for it in the calling context. The caller is responsible for freeing the allocated memory.

The function supports optional error handling through the missing_ok parameter - when set to false, it will raise an error if the origin doesn't exist; when true, it returns false and sets the output name to NULL.

## Parameters / Member Variables
- `roident`: The internal OID (RepOriginId) of the replication origin to look up
- `missing_ok`: If true, return false when origin not found; if false, raise an error when origin not found
- `roname`: Output parameter that receives a pointer to the palloc'd origin name string (caller must free)

## Dependencies
- Functions called/Symbols referenced:
  - `OidIsValid`: Validates that the provided OID is valid
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches the system cache for the origin catalog entry
  - `[text_to_cstring](../t/text_to_cstring.md)`: Converts PostgreSQL text type to C string
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases the system cache tuple
  - `ereport`: Reports errors when origin is not found and missing_ok is false
- Called from (representative examples):
  - [send_repl_origin](../s/send_repl_origin.md): When sending replication origin information in logical replication output
  - Various catalog and administrative functions that need to display origin names

## Notes and Other Information
- The returned name string is palloc'd in the calling context and must be freed by the caller
- Function validates input to reject InvalidRepOriginId and DoNotReplicateId special values
- Uses system cache for efficient lookup performance
- Returns true if origin found, false otherwise
- When missing_ok is false, the function will ereport(ERROR) for non-existent origins
- The roname output parameter is set to NULL when the origin is not found

## Simplified Source

```c
bool
replorigin_by_oid(RepOriginId roident, bool missing_ok, char **roname)
{
    Assert(OidIsValid((Oid) roident));
    Assert(roident != InvalidRepOriginId);
    Assert(roident != DoNotReplicateId);

    // Search system cache for replication origin
    HeapTuple tuple = SearchSysCache1(REPLORIGIDENT, ObjectIdGetDatum((Oid) roident));

    if (HeapTupleIsValid(tuple)) {
        // Extract name from catalog tuple
        Form_pg_replication_origin ric = (Form_pg_replication_origin) GETSTRUCT(tuple);
        *roname = text_to_cstring(&ric->roname);
        ReleaseSysCache(tuple);
        return true;
    } else {
        // Origin not found
        *roname = NULL;
        if (!missing_ok)
            ereport(ERROR, "replication origin with ID %d does not exist", roident);
        return false;
    }
}
```