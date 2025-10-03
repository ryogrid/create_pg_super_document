# ecpg_free_params

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1106-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1106-L1126)

## Overview
Frees all parameter-related memory allocations for an ECPG statement and optionally logs parameter values for debugging purposes.

## Definition

```c
void
ecpg_free_params(struct statement *stmt, bool print)
```
## Detailed Description
This function performs cleanup of all parameter-related data structures associated with an ECPG prepared statement. It iterates through all parameters, optionally logging their values for debugging, then systematically frees all allocated memory including parameter values, lengths, and formats arrays. The function also resets the parameter-related fields in the statement structure to ensure clean state.

## Parameters / Member Variables
- `*stmt`: Pointer to the statement structure containing parameter data to be freed
- `print`: Boolean flag indicating whether to log parameter values before freeing (used for debugging)
## Dependencies
- Functions called/Symbols referenced:
  - [print_param_value](../p/print_param_value.md)
  - [ecpg_free](ecpg_free.md)
- Called from (representative examples):
  - [ecpg_build_params](ecpg_build_params.md) (multiple locations)
  - [ecpg_autostart_transaction](ecpg_autostart_transaction.md)
  - [ecpg_execute](ecpg_execute.md)

## Notes and Other Information
- This function is essential for preventing memory leaks in ECPG parameter handling
- The print parameter enables debugging by logging all parameter values before cleanup
- After execution, the statement structure's parameter fields are reset to NULL/0 to prevent dangling pointers
- The function handles cleanup of three parallel arrays: paramvalues, paramlengths, and paramformats
- Called extensively throughout the ECPG execution flow wherever parameter cleanup is needed