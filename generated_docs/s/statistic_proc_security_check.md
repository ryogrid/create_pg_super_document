# statistic_proc_security_check

## Location
[src/backend/utils/adt/selfuncs.c:5801-5829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L5801-L5829)

## Overview
Determines whether it is safe to call a function with pg_statistic data, ensuring that statistical information is only exposed to authorized users or through leak-proof functions.

## Definition

```c
bool
statistic_proc_security_check(VariableStatData *vardata, Oid func_oid)
```
## Detailed Description
This function implements a security check that prevents unauthorized access to sensitive statistical data stored in pg_statistic. It serves as a gatekeeper that ensures statistical information can only be accessed under two specific conditions:

1. **Authorized Access**: The user has proper SELECT privileges on the underlying table/column and there are no security qualifiers (from security barrier views or RLS policies) that would restrict access
2. **Leak-proof Functions**: The function being called is marked as leak-proof, meaning it cannot expose information about its inputs through error messages, output, or side effects

This mechanism prevents potential information leakage where statistical data could reveal information about table contents to users who don't have permission to see the actual data.

## Parameters / Member Variables
- `*vardata`: VariableStatData structure containing statistical information and access control metadata (specifically the acl_ok field)
- `func_oid`: Object identifier of the function that wants to access the statistical data
## Dependencies
- Functions called/Symbols referenced:
  - [get_func_leakproof](../g/get_func_leakproof.md) (check if function is marked as leak-proof)
  - [get_func_name](../g/get_func_name.md) (retrieve function name for debug messages)
  - ereport (logging and error reporting)
- Called from (representative examples):
  - [var_eq_const](../v/var_eq_const.md) (variable equality selectivity estimation)
  - [mcv_selectivity](../m/mcv_selectivity.md) (most common values selectivity)
  - [histogram_selectivity](../h/histogram_selectivity.md) (histogram-based selectivity)
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md) (inequality histogram selectivity)
  - [eqjoinsel](../e/eqjoinsel.md) (equality join selectivity)
  - [get_variable_range](../g/get_variable_range.md) (variable range estimation)

## Notes and Other Information
- Returns true immediately if vardata->acl_ok is set, indicating the user has proper permissions and no security restrictions apply
- For unauthorized access, only allows leak-proof functions to proceed, preventing potential information disclosure
- Logs a DEBUG2 message when denying access to non-leak-proof functions, which helps with debugging selectivity estimation issues
- This security mechanism is critical for maintaining data privacy in multi-tenant environments or systems with row-level security
- The leak-proof function concept ensures that even if a function processes statistical data, it cannot inadvertently reveal information about the underlying data distribution to unauthorized users

## Simplified Source

```c
bool
statistic_proc_security_check(VariableStatData *vardata, Oid func_oid)
{
    // Allow access if user has proper SELECT privileges and no security restrictions
    if (vardata->acl_ok)
        return true;

    if (!OidIsValid(func_oid))
        return false;

    // Allow leak-proof functions to access statistics
    if (get_func_leakproof(func_oid))
        return true;

    // Log denial for debugging
    ereport(DEBUG2, (errmsg_internal("not using statistics because function \"%s\" is not leak-proof",
                                   get_func_name(func_oid))));
    return false;
}
```