# CheckSubscriptionRelkind

## Location
[src/backend/executor/execReplication.c:743-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execReplication.c#L743-L752)

## Overview
A validation function that checks whether a relation kind is supported as a logical replication target, ensuring only regular tables and partitioned tables can be used for subscription operations.

## Definition

```c
void
CheckSubscriptionRelkind(char relkind, const char *nspname,
						 const char *relname)
```
## Detailed Description
CheckSubscriptionRelkind is a utility function in PostgreSQL's logical replication system that validates whether a specific relation kind (relkind) can be used as a target for logical replication operations. The function enforces that only regular relations (RELKIND_RELATION) and partitioned tables (RELKIND_PARTITIONED_TABLE) are supported as logical replication targets. If an unsupported relation kind is encountered, the function raises an error with appropriate error codes and messages.

This function is part of the executor's replication infrastructure and serves as a critical validation point to prevent logical replication operations on unsupported object types like views, indexes, sequences, or other non-table objects.

## Parameters / Member Variables
- `relkind`: A character representing the relation kind to be validated (e.g., 'r' for regular table, 'p' for partitioned table)
- `*nspname`: The namespace (schema) name of the relation, used only for error reporting
- `*relname`: The relation name, used only for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_RELATION (constant for regular table relation kind)
  - RELKIND_PARTITIONED_TABLE (constant for partitioned table relation kind)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md) (utility function for generating relation kind error details)
- Called from (representative examples):
  - [CreateSubscription](CreateSubscription.md) (in subscription creation commands)
  - [logicalrep_rel_open](../l/logicalrep_rel_open.md) (when opening relations for logical replication)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md) (during tuple routing in logical replication worker)
  - [exec_rt_fetch](../e/exec_rt_fetch.md) (in executor runtime fetch operations)

## Notes and Other Information
- The function only allows RELKIND_RELATION and RELKIND_PARTITIONED_TABLE as valid targets for logical replication
- Error reporting includes both a general error message and specific details about why the relation kind is not supported
- The nspname and relname parameters are purely for error reporting and do not affect the validation logic
- This function is a key component in PostgreSQL's logical replication security and consistency model
- Located in src/backend/executor/execReplication.c at lines 743-752

## Simplified Source

```c
void
CheckSubscriptionRelkind(char relkind, const char *nspname, const char *relname)
{
    // Only regular tables and partitioned tables are supported for logical replication
    if (relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
             errmsg("cannot use relation \"%s.%s\" as logical replication target",
                    nspname, relname),
             errdetail_relkind_not_supported(relkind)));
}
```