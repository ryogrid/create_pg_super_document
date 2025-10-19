# pg_get_userbyid

## Location
[src/backend/utils/adt/ruleutils.c:2749-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2749-L2786)

## Overview
Retrieves the role name for a given role OID, providing a fallback display format when the role does not exist or is not accessible.

## Definition
```c
Datum pg_get_userbyid(PG_FUNCTION_ARGS)
```

## Detailed Description
pg_get_userbyid is a PostgreSQL system function that converts a role OID into its corresponding role name. It performs a lookup in the pg_authid system catalog to retrieve the role name associated with the provided OID. If the role exists and is accessible, it returns the actual role name. If the role cannot be found (either because it doesn't exist or due to access restrictions), the function returns a fallback string in the format "unknown (OID=n)" where n is the original OID value.

This function is particularly useful for system administration queries and information schema views that need to display human-readable role names instead of internal OID values, while gracefully handling cases where roles may have been dropped or are not visible to the current user.

## Parameters / Member Variables
- `roleid`: OID of the role whose name should be retrieved from pg_authid

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro for extracting OID argument)
  - [palloc](palloc.md) (PostgreSQL memory allocation)
  - memset (memory initialization)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup by OID)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (converts OID to Datum)
  - HeapTupleIsValid (checks if tuple lookup succeeded)
  - GETSTRUCT (extracts structure from heap tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache entry)
  - sprintf (formats fallback string)
  - PG_RETURN_NAME (macro for returning NAME result)
- Called from:
  - SQL function pg_get_userbyid() available to users

## Notes and Other Information
- This function is exposed as a SQL-callable system function in PostgreSQL
- Uses system cache lookup for efficient access to pg_authid
- Always returns a valid NAME result, never NULL
- The fallback format preserves the original OID for debugging purposes
- Allocates exactly NAMEDATALEN bytes for the result name
- Located in src/backend/utils/adt/ruleutils.c:2749-2786
- Handles both existing and non-existing roles gracefully

## Simplified Source

```c
Datum pg_get_userbyid(PG_FUNCTION_ARGS) {
    Oid roleid = PG_GETARG_OID(0);
    Name result;

    // Allocate and initialize result buffer
    result = (Name) palloc(NAMEDATALEN);
    memset(NameStr(*result), 0, NAMEDATALEN);

    // Look up role in pg_authid catalog
    HeapTuple roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (HeapTupleIsValid(roletup)) {
        // Role found: extract and return role name
        Form_pg_authid role_rec = (Form_pg_authid) GETSTRUCT(roletup);
        *result = role_rec->rolname;
        ReleaseSysCache(roletup);
    }
    else {
        // Role not found: return fallback format
        sprintf(NameStr(*result), "unknown (OID=%u)", roleid);
    }

    PG_RETURN_NAME(result);
}
```