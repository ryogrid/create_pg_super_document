# selectDumpableDefaultACL

## Location
[src/bin/pg_dump/pg_dump.c:1954-1975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1954-L1975)

## Overview
This function determines whether a default ACL (Access Control List) should be included in a PostgreSQL database dump based on dump configuration and schema selection criteria.

## Definition

```c
static void
selectDumpableDefaultACL(DefaultACLInfo *dinfo, DumpOptions *dopt)
```
## Detailed Description
The selectDumpableDefaultACL function implements the policy logic for deciding whether to include default ACLs in database dumps. Default ACLs define the privileges that are automatically granted on newly created objects within a schema or database. The function evaluates two main scenarios:

1. **Per-schema default ACLs**: If the default ACL is associated with a specific schema, it will be dumped only if that schema is also being dumped
2. **Global default ACLs**: If the default ACL is not tied to a specific schema, it will be dumped only if the dump includes everything (controlled by the include_everything option)

The function sets the dump component flags appropriately based on these conditions, with the understanding that additional checks for dataOnly and aclsSkip options are performed elsewhere in the dumping process.

## Parameters / Member Variables
- `*dinfo`: Pointer to DefaultACLInfo structure containing information about the default ACL being evaluated
- `*dopt`: Pointer to DumpOptions structure containing the dump configuration settings
## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions (structure)
  - DefaultACLInfo (structure)
  - DUMP_COMPONENT_ALL (constant)
  - DUMP_COMPONENT_NONE (constant)
- Called from (representative examples):
  - [getDefaultACLs](../g/getDefaultACLs.md)

## Notes and Other Information
- Default ACLs cannot be extension members, as noted in the function comment
- The function is static and only used internally within pg_dump.c
- The actual ACL content filtering (dataOnly, aclsSkip) is handled by separate mechanisms
- Per-schema default ACLs are considered part of their containing namespace for dump purposes