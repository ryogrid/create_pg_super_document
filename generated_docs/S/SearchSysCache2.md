# SearchSysCache2

## Location
src/backend/utils/cache/syscache.c: 232 - 242

## Overview
A specialized wrapper around SearchCatCache2 optimized for system catalog caches that use exactly two search keys.

## Definition


## Detailed Description
SearchSysCache2 is a type-safe convenience function that provides access to system catalog caches that are indexed by exactly two keys. It serves as a specialized version of SearchSysCache, but with compile-time enforcement that the target cache uses exactly two search keys.

The function validates that the specified cache is configured for two-key lookups through an assertion check (cc_nkeys == 2), providing additional safety compared to the general SearchSysCache function. This helps prevent runtime errors when attempting to use two-key searches on caches with different key counts.

This function is extensively used throughout PostgreSQL for accessing commonly-needed two-key catalog lookups, such as finding attributes by relation OID and attribute name, or accessing various catalog entries that require composite keys.

## Parameters / Member Variables
- : Integer identifier specifying which system cache to search (must be valid two-key cache)
- : First search key value used in the composite lookup
- : Second search key value used in the composite lookup

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [SearchCatCache2](SearchCatCache2.md)
- Called from (representative examples):
  - [expand_all_col_privileges](../e/expand_all_col_privileges.md)
  - [ExecGrant_Attribute](../E/ExecGrant_Attribute.md)
  - [pg_attribute_aclmask_ext](../p/pg_attribute_aclmask_ext.md)
  - [get_attname](../g/get_attname.md)
  - [get_atttype](../g/get_atttype.md)
  - [SearchSysCacheAttName](SearchSysCacheAttName.md)
  - [SearchSysCacheAttNum](SearchSysCacheAttNum.md)

## Notes and Other Information
- Enforces two-key cache constraint through assertion (cc_nkeys == 2)
- More type-safe than using SearchSysCache with unused key3, key4 parameters
- Heavily used for attribute-related lookups (relation OID + attribute name/number)
- Commonly used for ACL checks, foreign key operations, and metadata retrieval
- The returned tuple is a cache copy and must NOT be freed by the caller
- Must call ReleaseSysCache() when finished with the returned tuple
- Part of the performance-critical path for query processing and DDL operations
- One of the most frequently used cache search functions in PostgreSQL codebase