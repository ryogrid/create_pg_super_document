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