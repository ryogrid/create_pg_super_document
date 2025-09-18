# contain_leaked_vars_checker

## Location
src/backend/optimizer/util/clauses.c: 1269 - 1274

## Overview
The `contain_leaked_vars_checker` function is a helper function that determines whether a given function is leakproof or not by checking its leakproof property.

## Definition
```c
static bool contain_leaked_vars_checker(Oid func_id, void *context)
```

## Detailed Description
This static function serves as a checker callback used within the variable leakage detection mechanism. It examines a specific function identified by its OID and determines if it is non-leakproof (i.e., potentially leaky). The function returns true if the function is NOT leakproof, meaning it could potentially leak sensitive information.

This function is designed to be used as a callback function in conjunction with tree-walking functions that need to identify potentially dangerous functions during query optimization. It's part of the security infrastructure that helps PostgreSQL maintain data confidentiality in the presence of security barriers.

## Parameters / Member Variables
- `func_id`: An Oid representing the function identifier to be checked for leakproof property
- `context`: A void pointer for additional context information (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [get_func_leakproof](../g/get_func_leakproof.md)
- Called from (representative examples):
  - [contain_leaked_vars_walker](contain_leaked_vars_walker.md)

## Notes and Other Information
- Returns true if the function is NOT leakproof (i.e., potentially leaky)
- This function is static and only used internally within clauses.c
- The context parameter is not used in the current implementation but follows a standard callback pattern
- Part of PostgreSQL's security barrier mechanism to prevent data leakage
- Located in src/backend/optimizer/util/clauses.c:1269-1274