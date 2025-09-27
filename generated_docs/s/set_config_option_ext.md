# set_config_option_ext

## Location
[src/backend/utils/misc/guc.c:3385-3407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L3385-L3407)

## Overview
Extended version of set_config_option that allows explicit specification of the role OID responsible for setting the configuration parameter.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function provides an extended interface to set_config_option by adding explicit control over the role OID (srole parameter) that is considered responsible for setting the configuration parameter. While set_config_option automatically determines the appropriate role based on the GucSource, this extended version allows callers to override that decision.

This capability is essential for several scenarios: when restoring previously-assigned values where maintaining the original setter's identity is crucial, when the value was determined through special mechanisms that don't map cleanly to standard GucSource categories, and when internal GUC system operations need precise control over role attribution.

The function is a thin wrapper around set_config_with_handle, passing through all parameters including the explicitly specified srole. This design maintains consistency with the broader GUC architecture while providing the additional flexibility needed by internal PostgreSQL components.

## Parameters / Member Variables
- : The configuration parameter name to set
- : The new value as a string (NULL means set to default value)  
- : The GUC context level determining access requirements
- : Source of the configuration change for logging and validation purposes
- : Explicit role OID that should be considered as setting this value
- : Whether to set globally, locally to current transaction, or just for function duration
- : If false, perform validation only without actually changing the value
- : Error reporting level to use, or 0 for automatic choice
- : True when loading settings from another process

## Dependencies
- Functions called/Symbols referenced:
  - GucContext, GucSource, GucAction (enum types)
  - [set_config_with_handle](set_config_with_handle.md) (core implementation function)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [reapply_stacked_values](../r/reapply_stacked_values.md) (restoring previous settings)
  - [read_nondefault_variables](../r/read_nondefault_variables.md) (loading saved configurations)
  - [RestoreGUCState](../R/RestoreGUCState.md) (state restoration after parallel operations)
  - [define_custom_variable](../d/define_custom_variable.md) (custom parameter registration)
  - [InitializeWalConsistencyChecking](../I/InitializeWalConsistencyChecking.md) (WAL consistency setup)

## Notes and Other Information
- Provides explicit control over role attribution for configuration changes
- Essential for state restoration operations where original setter identity must be preserved
- Used primarily by internal GUC system components rather than external callers
- Recommended srole values: GetUserId() for SQL operations, BOOTSTRAP_SUPERUSERID for config files
- Maintains same return value semantics as set_config_option (+1, 0, -1)
- Critical for parallel query operations where GUC state must be synchronized across processes
- Part of PostgreSQL's role-based security model for configuration management
- Enables precise audit trails for configuration changes by preserving original setter information

## Simplified Source

```c
// Simplified version of set_config_option_ext
int set_config_option_ext(const char *name, const char *value,
                         GucContext context, GucSource source, Oid srole,
                         GucAction action, bool changeVal, int elevel,
                         bool is_reload) {
    // This is a wrapper function that adds explicit role OID control
    // to the standard configuration setting mechanism

    // Simply delegates to the core implementation with all parameters
    return set_config_with_handle(name, NULL, value,
                                 context, source, srole,
                                 action, changeVal, elevel,
                                 is_reload);
}
```

Key simplifications made:
- Preserved the complete function signature as it's essential for understanding
- Added clear comments explaining the wrapper nature
- Maintained the single delegation call to show the actual implementation
- Focused on the core purpose: providing explicit role OID control