# ParameterAclLookup

## Location
[src/backend/catalog/pg_parameter_acl.c:35-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_parameter_acl.c#L35-L67)

## Overview
ParameterAclLookup looks up the OID of a configuration parameter's Access Control List (ACL) entry in the PostgreSQL system catalog, providing a way to retrieve ACL information for database configuration parameters.

## Definition


## Detailed Description
This function searches for a configuration parameter's ACL entry in the pg_parameter_acl system catalog and returns its OID. The function first converts the parameter name to the standardized form used in the catalog using convert_GUC_name_for_parameter_acl, then performs a system cache lookup using the PARAMETERACLNAME cache. If the ACL entry is not found and missing_ok is false, it throws an ERROR with ERRCODE_UNDEFINED_OBJECT. The function ensures proper memory cleanup by freeing the converted parameter name before returning.

## Parameters / Member Variables
- : The name of the configuration parameter to look up in the ACL system
- : Boolean flag controlling error behavior - if false, throws error when ACL not found; if true, returns InvalidOid silently

## Dependencies
- Functions called/Symbols referenced:
  - [convert_GUC_name_for_parameter_acl](../c/convert_GUC_name_for_parameter_acl.md)
  - GetSysCacheOid1
  - cstring_to_text
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md)
  - [get_object_address_unqualified](../g/get_object_address_unqualified.md)

## Notes and Other Information
- The function performs proper memory management by calling pfree() on the converted parameter name
- Uses the PARAMETERACLNAME system cache for efficient lookups
- Part of PostgreSQL's Row Level Security (RLS) and parameter access control infrastructure
- Returns InvalidOid when the ACL entry is not found and missing_ok is true
- Error reporting follows PostgreSQL's standard error handling patterns with appropriate error codes