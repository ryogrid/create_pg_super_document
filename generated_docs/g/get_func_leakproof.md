# get_func_leakproof

## Location
[src/backend/utils/cache/lsyscache.c:1837-1857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1837-L1857)

## Overview
Returns the leakproof field of a function given its OID, used to determine whether a function is safe to execute with sensitive data in security-restricted contexts.

## Definition
```c
bool get_func_leakproof(Oid funcid)
```

## Detailed Description
This function retrieves the leakproof flag for a PostgreSQL function from the system catalog. A leakproof function is one that cannot leak information about its inputs through error messages or return values, making it safe to use in security-sensitive contexts such as row-level security policies and views. The function performs a system cache lookup on the pg_proc catalog to retrieve the proleakproof field.

## Parameters / Member Variables
- `funcid`: The OID of the function to check for the leakproof property

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_proc
- Called from (representative examples):
  - [select_equality_operator](../s/select_equality_operator.md)
  - [contain_leaked_vars_checker](../c/contain_leaked_vars_checker.md)
  - [contain_leaked_vars_walker](../c/contain_leaked_vars_walker.md)
  - [statext_is_compatible_clause_internal](../s/statext_is_compatible_clause_internal.md)
  - [statistic_proc_security_check](../s/statistic_proc_security_check.md)

## Notes and Other Information
- The function raises an ERROR if the function OID is not found in the system cache
- This is part of PostgreSQL's security infrastructure for determining which functions can be safely executed in restricted contexts
- The leakproof property is critical for row-level security and other security-sensitive operations