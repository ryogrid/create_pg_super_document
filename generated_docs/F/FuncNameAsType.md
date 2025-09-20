# FuncNameAsType

## Location
[src/backend/parser/parse_func.c:1881-1911](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1881-L1911)

## Overview
A convenience routine that checks if a function name matches an existing type name, returning the type's OID if found.

## Definition

```c
static Oid
FuncNameAsType(List *funcname)
```
## Detailed Description
The  function serves as a utility to determine whether a given function name corresponds to an existing PostgreSQL type. This is particularly useful during function resolution when the parser needs to distinguish between function calls and type casting operations. The function performs a type lookup and validates that the type is properly defined and not just a shell type or complex type that should be ignored.

The function uses  in the type lookup to maintain security contracts when writing SECURITY DEFINER functions safely. It only returns valid type OIDs for fully defined types that are not composite types.

## Parameters / Member Variables
- : List of strings representing the qualified or unqualified function/type name to check

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md)
  - makeTypeNameFromNameList
  - [typeTypeId](../t/typeTypeId.md)
  - [typeTypeRelid](../t/typeTypeRelid.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Type (struct)
  - Form_pg_type (struct)
- Called from (representative examples):
  - [func_get_detail](../f/func_get_detail.md)
  - FuncLookupError

## Notes and Other Information
- Returns  if no matching type is found or if the type is a shell type or complex type
- Uses  parameter in type lookup for security reasons when dealing with SECURITY DEFINER functions
- The function is static, meaning it's only used within the same translation unit (parse_func.c)
- Performs proper cleanup by releasing the system cache tuple after use
- Checks both that the type is defined () and that it's not a composite type (via  validation)