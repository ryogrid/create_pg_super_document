# aclexplode

## Location
src/backend/utils/adt/acl.c: 1791 - 1894

## Overview
Converts an ACL (Access Control List) array into a tabular format, with one row per individual privilege grant.

## Definition
```c
Datum aclexplode(PG_FUNCTION_ARGS)
```

## Detailed Description
The `aclexplode` function is a set-returning function (SRF) that decomposes an ACL array into a detailed table format. Each row in the result represents a single privilege grant, showing the grantor, grantee, privilege type, and whether the grant includes the grant option. This function is essential for introspection of PostgreSQL's privilege system, allowing users and administrators to see exactly which privileges have been granted and by whom.

The function works by:
1. Iterating through each ACL item in the input array
2. For each ACL item, iterating through all possible privilege bits
3. For each privilege bit that is set, creating a result row
4. Including grant option information for each privilege

The output table has four columns: grantor (OID), grantee (OID), privilege_type (text), and is_grantable (boolean). This detailed breakdown makes it easy to understand complex privilege structures and relationships.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Acl*): The ACL array to explode into tabular format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACL_P (macro for extracting ACL argument)
  - SRF_IS_FIRSTCALL/SRF_FIRSTCALL_INIT (set-returning function macros)
  - SRF_PERCALL_SETUP/SRF_RETURN_NEXT/SRF_RETURN_DONE (SRF control macros)
  - check_acl (validates ACL structure)
  - CreateTemplateTupleDesc/TupleDescInitEntry/BlessTupleDesc (tuple descriptor creation)
  - ACL_NUM/ACL_DAT (macros for accessing ACL components)
  - ACLITEM_GET_PRIVS/ACLITEM_GET_GOPTIONS (privilege extraction macros)
  - convert_aclright_to_string (converts privilege bits to strings)
  - heap_form_tuple/HeapTupleGetDatum (tuple construction)
  - Memory management functions (palloc, MemoryContextSwitchTo)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a PostgreSQL SQL function for ACL introspection
- Implements the set-returning function protocol for returning multiple rows
- Uses a state machine approach with persistent context between calls
- Each privilege bit is examined individually, creating separate rows for each granted privilege
- Handles grant options separately, showing which privileges can be further granted
- Memory management follows PostgreSQL's multi-call function context pattern
- Essential for database administration tools and privilege auditing
- The function can handle empty ACLs gracefully
- Result format matches PostgreSQL's standard privilege display conventions
- Used by system views and administrative functions for privilege reporting