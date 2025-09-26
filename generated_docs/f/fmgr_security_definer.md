# fmgr_security_definer

## Location
[src/backend/utils/fmgr/fmgr.c:632-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L632-L791)

## Overview
A function handler that implements security-definer execution, configuration parameter management, and plugin hooks for PostgreSQL functions requiring elevated privileges or special execution contexts.

## Definition

```c
struct fmgr_security_definer_cache *volatile fcache;
```
## Detailed Description
The  function serves as a sophisticated wrapper for executing PostgreSQL functions that require special security or configuration contexts. It handles three main responsibilities: security-definer execution (running functions with the privileges of their owner rather than the caller), applying function-specific configuration parameters (proconfig), and invoking function manager hooks for plugins.

The function operates by creating a cache of execution context information on first invocation, then temporarily switching user context and configuration settings before calling the actual target function. It uses PostgreSQL's exception handling system (PG_TRY/PG_CATCH) to ensure proper cleanup of security contexts even when the wrapped function fails.

The security-definer mechanism allows functions to execute with the privileges of their creator (owner) rather than the current user, enabling controlled privilege escalation. Configuration parameters allow functions to temporarily override GUC (Grand Unified Configuration) settings during execution. Plugin hooks provide extensibility points for third-party code to monitor or modify function execution.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that expands to 

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md)  
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [TransformGUCArray](../T/TransformGUCArray.md)
  - [get_config_handle](../g/get_config_handle.md)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md)
  - [set_config_with_handle](../s/set_config_with_handle.md)
  - FunctionCallInvoke
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md)
  - [pgstat_end_function_usage](../p/pgstat_end_function_usage.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
- Called from (representative examples):
  - [fmgr_info_cxt_security](fmgr_info_cxt_security.md)

## Notes and Other Information
- Caches execution context information in fn_extra for performance
- Handles both security-definer and configuration parameter functionality
- Uses PostgreSQL's GUC nesting mechanism for configuration changes
- Implements proper exception handling to restore security contexts on errors
- Supports function manager hooks for plugin extensibility
- Not re-entrant due to flinfo manipulation, but fcinfo itself isn't re-entrant either
- Critical component for PostgreSQL's function security and configuration system
- Manages statistics collection for wrapped function execution