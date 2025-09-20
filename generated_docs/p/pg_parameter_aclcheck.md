# pg_parameter_aclcheck

## Location
[src/backend/catalog/aclchk.c:4121-4132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4121-L4132)

## Overview
Checks a user's access privileges to a configuration parameter (GUC) identified by name, providing a simple interface for parameter access control validation.

## Definition

```c
AclResult
pg_parameter_aclcheck(const char *name, Oid roleid, AclMode mode)
```
## Detailed Description
This function serves as an exported routine for checking access privileges to PostgreSQL configuration parameters (GUCs). It acts as a wrapper around the more complex  function, simplifying the access control check by returning a straightforward success/failure result. The function determines whether a given role has the specified access mode to a named configuration parameter.

## Parameters / Member Variables
- : The name of the configuration parameter (GUC) to check access for
- : The OID of the role whose privileges are being checked
- : The type of access being requested (AclMode enumeration)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_parameter_aclmask](pg_parameter_aclmask.md)
  - ACLMASK_ANY
  - ACLCHECK_NO_PRIV
  - AclResult
- Called from (representative examples):
  - [has_param_priv_byname](../h/has_param_priv_byname.md)
  - [set_config_with_handle](../s/set_config_with_handle.md)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)
  - validate_option_array_item

## Notes and Other Information
- Located in src/backend/catalog/aclchk.c:4121-4132
- This function provides a boolean-like interface to parameter privilege checking
- Returns ACLCHECK_OK if the role has the required privileges, ACLCHECK_NO_PRIV otherwise
- Part of PostgreSQL's access control system for configuration parameters
- Used extensively in GUC (Grand Unified Configuration) system for parameter validation