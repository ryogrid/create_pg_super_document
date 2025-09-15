# set_config_option_ext

## Overview
Extended interface for setting PostgreSQL configuration parameters with explicit role specification, enabling precise control over privilege contexts during parameter modification.

## Definition
```c
int set_config_option_ext(const char *name, const char *value, GucContext context, GucSource source, Oid srole, GucAction action, bool changeVal, int elevel, bool is_reload)
```

## Detailed Description
This function provides fine-grained control over role-based security during configuration parameter modification by accepting an explicit srole parameter.

## Parameters / Member Variables
- `name`: Name of the GUC parameter to modify
- `value`: String representation of the new parameter value
- `context`: GucContext enumeration specifying scope and access level
- `source`: GucSource enumeration indicating origin of change
- `srole`: Explicit role OID for privilege checking and ownership tracking
- `action`: GucAction enumeration controlling how setting is applied
- `changeVal`: Boolean flag indicating whether to apply the change
- `elevel`: Error reporting level for validation failures
- `is_reload`: Boolean indicating if part of configuration reload

## Dependencies
- **Functions called/Symbols referenced**:
  - `set_config_with_handle` - Core configuration function
- **Called from (representative examples)**:
  - `RestoreGUCState` - Configuration restoration function
  - `read_nondefault_variables` - Configuration loading function

## Notes & Other Information
Primarily used by internal PostgreSQL subsystems requiring precise control over configuration security contexts.