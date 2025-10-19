# aclitemout

## Location
[src/backend/utils/adt/acl.c:646-712](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L646-L712)

## Overview
Converts an AclItem structure into its external string representation for display and storage purposes.

## Definition
```c
Datum aclitemout(PG_FUNCTION_ARGS)
```

## Detailed Description
The aclitemout function is a PostgreSQL output function that converts an internal AclItem structure into a human-readable string format. The output format follows the pattern "grantee=privileges/grantor" where grantee and grantor are role names (or numeric OIDs if the role doesn't exist), and privileges are represented as a string of characters corresponding to different access rights. The function handles special cases like public access (empty grantee) and looks up role names from the system catalog. Grant options are indicated with asterisks (*) following the corresponding privilege characters.

## Parameters / Member Variables
- Input via PG_FUNCTION_ARGS:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACLITEM_P (macro to extract AclItem argument)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure extraction)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - [putid](../p/putid.md) (utility function for ID formatting)
  - sprintf (standard string formatting)
  - ACLITEM_GET_PRIVS (macro to extract privileges)
  - ACLITEM_GET_GOPTIONS (macro to extract grant options)
  - PG_RETURN_CSTRING (macro to return C string as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This is a PostgreSQL output function for the aclitem data type
- Generates string format: "grantee=privileges/grantor"
- Uses role names when available, falls back to numeric OIDs for missing roles
- Grant options are represented with asterisks (*) after privilege characters
- Empty grantee indicates public access (ACL_ID_PUBLIC)
- Allocates sufficient buffer space for maximum possible output length
- Part of PostgreSQL's type system infrastructure for ACL item input/output
- Privilege characters are defined in ACL_ALL_RIGHTS_STR constant

## Simplified Source

```c
Datum aclitemout(PG_FUNCTION_ARGS) {
    AclItem *aip = PG_GETARG_ACLITEM_P(0);
    char *out, *p;
    HeapTuple htup;
    unsigned i;

    // Allocate buffer for output string
    out = palloc(strlen("=/") + 2 * N_ACL_RIGHTS + 2 * (2 * NAMEDATALEN + 2) + 1);
    p = out;
    *p = '\0';

    // Format grantee (role name or OID)
    if (aip->ai_grantee != ACL_ID_PUBLIC) {
        htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aip->ai_grantee));
        if (HeapTupleIsValid(htup)) {
            putid(p, NameStr(((Form_pg_authid) GETSTRUCT(htup))->rolname));
            ReleaseSysCache(htup);
        } else {
            sprintf(p, "%u", aip->ai_grantee);  // Use OID if role not found
        }
    }

    // Move to end of grantee string
    while (*p) ++p;

    *p++ = '=';

    // Format privileges and grant options
    for (i = 0; i < N_ACL_RIGHTS; ++i) {
        if (ACLITEM_GET_PRIVS(*aip) & (UINT64CONST(1) << i))
            *p++ = ACL_ALL_RIGHTS_STR[i];
        if (ACLITEM_GET_GOPTIONS(*aip) & (UINT64CONST(1) << i))
            *p++ = '*';  // Mark grant option
    }

    *p++ = '/';
    *p = '\0';

    // Format grantor (role name or OID)
    htup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(aip->ai_grantor));
    if (HeapTupleIsValid(htup)) {
        putid(p, NameStr(((Form_pg_authid) GETSTRUCT(htup))->rolname));
        ReleaseSysCache(htup);
    } else {
        sprintf(p, "%u", aip->ai_grantor);  // Use OID if role not found
    }

    PG_RETURN_CSTRING(out);
}
```