# PQparamtype

## Location
[src/interfaces/libpq/fe-exec.c:3926-3943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3926-L3943)

## Overview
PQparamtype returns the PostgreSQL data type OID of a specified parameter in a prepared statement.

## Definition
```c
Oid PQparamtype(const PGresult *res, int param_num)
```

## Detailed Description
PQparamtype provides type information for parameters of prepared statements, allowing applications to understand what data types are expected for each parameter position. This is crucial for proper type handling when binding parameters to prepared statements.

The function performs parameter number validation using check_param_number() before accessing the type information. It returns the OID (Object Identifier) that corresponds to the PostgreSQL data type for the specified parameter. The type information is stored in the paramDescs array within the PGresult structure, which is populated when describing a prepared statement.

If the parameter number is invalid, the result pointer is NULL, or no parameter descriptors are available, the function returns InvalidOid (0) to indicate an error or unavailable type information.

## Parameters / Member Variables
- `res`: Pointer to the PGresult structure from a prepared statement description
- `param_num`: Zero-based parameter index to query for type information

## Dependencies
- Functions called/Symbols referenced:
  - [check_param_number](../c/check_param_number.md)
  - InvalidOid
- Called from (representative examples):
  - (Limited direct usage found in codebase - primarily used by client applications)

## Notes and Other Information
- Returns InvalidOid (0) if the parameter number is out of range or if descriptors are unavailable
- Only meaningful for PGresult structures from prepared statement descriptions (PQdescribePrepared)
- The returned OID can be used with other PostgreSQL type system functions to get detailed type information
- Essential for applications that need to perform type-specific parameter binding or validation
- Part of the prepared statement introspection API alongside PQnparams()
- The paramDescs field must be populated (typically by PQdescribePrepared) for this function to return valid type information

## Simplified Source

```c
Oid PQparamtype(const PGresult *res, int param_num)
{
    // Validate parameter number is in range
    if (!check_param_number(res, param_num))
        return InvalidOid;

    // Return parameter type OID if descriptors available
    if (res->paramDescs)
        return res->paramDescs[param_num].typid;
    else
        return InvalidOid;
}
```