# config_handle

## Location
[src/include/utils/guc.h:147-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc.h#L147-L168)

## Overview
config_handle is a typedef alias for struct config_generic, providing an opaque handle to configuration variables in PostgreSQL's GUC (Grand Unified Configuration) system.

## Definition

```c
typedef struct config_generic config_handle;
```
## Detailed Description
The config_handle type serves as an opaque handle for configuration variables, hiding the implementation details of the underlying config_generic structure from external code. This typedef provides a clean interface for functions that need to manipulate configuration variables without exposing the full complexity of the config_generic structure.

The underlying config_generic structure contains comprehensive metadata about configuration variables including their names, contexts, descriptions, current values, sources, and various state information. By using config_handle as an alias, PostgreSQL maintains encapsulation while providing a stable API for configuration management functions.

This design pattern allows for better maintainability and API stability, as the internal structure of config_generic can evolve without breaking external interfaces that use config_handle.

## Parameters / Member Variables
As this is a typedef alias, config_handle inherits all members from config_generic:
- : Name of the configuration variable (must be first field)
- : Context required to set the variable
- : Grouping for organizational purposes
- : Short description of the variable's purpose
- : Long description of the variable's purpose
- : Various flag bits controlling behavior
- : Type of variable (set at startup)
- : Status bits indicating current state
- : Source of the current value
- : Source of the reset value
- : Context that set the current value
- : Context that set the reset value
- : Role that set the current value
- : Role that set the reset value
- : Stack of prior values for transaction handling
- : Extra pointer for current actual value
- : List link for non-default variables
- : List link for variables with non-NULL stack
- : List link for variables needing reporting
- : Last value sent to client for GUC_REPORT variables
- : Configuration file where setting originates
- : Line number in source file

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](config_generic.md) (aliased structure type)
  - [ConfigVariable](../C/ConfigVariable.md) (used in related function prototypes)
- Called from (representative examples):
  - [set_config_with_handle](../s/set_config_with_handle.md)
  - EmitWarningsOnPlaceholders
  - [fmgr_security_definer](../f/fmgr_security_definer.md)

## Notes and Other Information
- This typedef provides encapsulation and API stability for configuration variable handling
- The handle concept allows external code to manipulate configuration variables without direct access to internal structure details
- Used primarily in functions that need to work with configuration variables in a generic way
- The underlying config_generic structure contains extensive metadata for comprehensive configuration management
- Related to configuration file parsing functions that work with ConfigVariable lists