# get_default_acl_internal

## Location
[src/backend/catalog/aclchk.c:4271-4305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4271-L4305)

## Overview
Fetches the default ACL (Access Control List) entry from pg_default_acl catalog for a specific role, namespace, and object type combination, returning the ACL or NULL if no entry exists.

## Definition

```c
static Acl *
get_default_acl_internal(Oid roleId, Oid nsp_oid, char objtype)
```
## Detailed Description
This internal static function performs a direct lookup in the pg_default_acl system catalog to retrieve default permissions for objects created by a specific role within a specific namespace. It uses the system cache (DEFACLROLENSPOBJ) for efficient access to default ACL entries. The function is designed as a low-level utility that provides the core lookup mechanism for PostgreSQL's default privilege system.

The function searches using a three-part key consisting of the role ID, namespace OID, and object type character. If a matching entry is found, it extracts the ACL data from the defaclacl column and returns a copy. The object type parameter must be encoded according to pg_default_acl's internal representation (e.g., 'r' for tables, 'S' for sequences, 'f' for functions, 'T' for types).

## Parameters / Member Variables
- : OID of the role whose default ACL is being queried
- : OID of the namespace (schema) for which the default ACL applies
- : Character code representing the object type using pg_default_acl encoding

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache3](../S/SearchSysCache3.md) (system cache lookup with 3 keys)
  - [CharGetDatum](../C/CharGetDatum.md) (converts char to Datum)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (extracts attribute from cached tuple)
  - DatumGetAclPCopy (creates a copy of ACL from Datum)
  - HeapTupleIsValid (validates tuple existence)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache reference)
- Called from:
  - [get_user_default_acl](get_user_default_acl.md) (twice - for role-specific and public default ACLs)

## Notes and Other Information
- This is a static function, only accessible within the aclchk.c file
- Returns NULL when no default ACL entry exists for the specified combination
- Uses PostgreSQL's system cache mechanism for performance optimization
- The returned ACL is a copy (via DatumGetAclPCopy), so the caller owns the memory
- Object type encoding follows pg_default_acl standards: 'r'=relations, 'S'=sequences, 'f'=functions, 'T'=types
- Part of PostgreSQL's default privilege infrastructure that allows setting permissions for future objects