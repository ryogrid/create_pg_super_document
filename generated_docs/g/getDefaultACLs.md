# getDefaultACLs

## Location
src/bin/pg_dump/pg_dump.c: 9846 - 9945

## Overview
Reads all default ACL information from the system catalogs and returns them in a structured format for pg_dump processing.

## Definition
```c
DefaultACLInfo *getDefaultACLs(Archive *fout, int *numDefaultACLs)
```

## Detailed Description
This function is part of the pg_dump utility and extracts default access control list (ACL) information from the PostgreSQL system catalog `pg_default_acl`. Default ACLs define the default privileges that will be assigned to newly created objects of specific types. The function handles two types of default ACLs: global entries (with defaclnamespace=0) that replace hard-wired defaults, and namespace-specific entries that only add privileges. Global entries are dumped as deltas from the system default ACL, while namespace-specific entries are dumped as-is (deltas from an empty ACL). The function processes special handling for sequence objects by converting 'S' to 's' for the acldefault() function call.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, containing dump options and used for executing SQL queries
- `numDefaultACLs`: Output parameter that receives the count of default ACLs found

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQfnumber/PQgetvalue
  - pg_malloc
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableDefaultACL](../s/selectDumpableDefaultACL.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function distinguishes between global default ACLs (defaclnamespace=0) and namespace-specific ACLs
- Global ACLs are computed as deltas from system defaults using the `acldefault()` function
- Namespace-specific ACLs use an empty ACL ({}) as their baseline
- Special case handling converts sequence object type 'S' to 's' for acldefault() compatibility
- Each default ACL automatically gets `DUMP_COMPONENT_ACL` since they are inherently ACL objects
- The object name is set to the defaclobjtype character for identification purposes
- Namespace resolution uses `findNamespace()` for non-global ACLs
- The returned `DefaultACLInfo` array must be freed by the caller
- Uses `selectDumpableDefaultACL()` which may have different criteria than regular `selectDumpableObject()`