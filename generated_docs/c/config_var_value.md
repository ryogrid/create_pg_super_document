# config_var_value

## Location
[src/include/utils/guc_tables.h:45-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc_tables.h#L45-L49)

## Overview
Represents the actual value of a GUC (Grand Unified Configuration) variable, including both the value itself and optional extra data created by hooks.

## Definition

```c
typedef struct config_var_value
{
	union config_var_val val;
	void	   *extra;
} config_var_value;
```
## Detailed Description
The config_var_value struct is a container for GUC variable values that combines the actual variable value with an optional opaque struct "extra". This extra data is created by the variable's check_hook and used by its assign_hook, providing a mechanism for storing additional context or processed data associated with the configuration variable.

## Parameters / Member Variables
- `val`: A union containing the actual value of the GUC variable, which can be of different types (bool, int, double, string, or enum)
- `*extra`: A void pointer to malloc'd opaque data created by the check_hook and used by the assign_hook for additional variable-specific processing
## Dependencies
- Types referenced:
  - config_var_val (union containing the actual variable value)
- Used by:
  - [set_stack_value](../s/set_stack_value.md) (function that sets values in GUC stack)
  - [discard_stack_value](../d/discard_stack_value.md) (function that discards stack values)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (end-of-transaction GUC cleanup)
  - [guc_stack](../g/guc_stack.md) (struct that maintains GUC value stack)

## Notes and Other Information
This structure is fundamental to PostgreSQL's GUC system, allowing configuration variables to have both their primary value and associated metadata or processed state. The extra field enables sophisticated variable validation and assignment logic through the hook mechanism.