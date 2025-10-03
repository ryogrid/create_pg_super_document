# ECPGfree_type

## Location
[src/interfaces/ecpg/preproc/type.c:655-692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/type.c#L655-L692)

## Overview
ECPGfree_type is a function that recursively frees ECPGtype structures and their associated memory, handling different type categories including arrays, structs, unions, and simple types.

## Definition

```c
void
ECPGfree_type(struct ECPGtype *type)
```
## Detailed Description
This function implements a comprehensive memory deallocation system for ECPGtype structures. It first checks if the type is a simple type using IS_SIMPLE_TYPE macro, and if not, it handles complex types through a switch statement. For array types, it recursively handles the element type, with special handling for arrays of structs/unions. For struct and union types, it calls ECPGfree_struct_member to free all member information. The function includes error handling for unexpected or unsupported type combinations like multidimensional arrays.

## Parameters / Member Variables
- `*type`: Pointer to the ECPGtype structure to be freed. The structure contains type information and a union of type-specific data
## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_TYPE (macro to check if type is simple)
  - [ECPGfree_struct_member](ECPGfree_struct_member.md) (frees struct member linked lists)
  - base_yyerror (parser error reporting)
  - mmerror (memory/parsing error reporting)  
  - free (standard C library deallocation)
  - ECPGt_array, ECPGt_struct, ECPGt_union (type enumeration values)
- Called from (representative examples):
  - [remove_variables](../r/remove_variables.md) (cleanup when removing variable definitions)

## Notes and Other Information
- Simple types (basic data types) require no special cleanup beyond freeing the ECPGtype structure itself
- Array types require recursive handling of their element types, with special error checking for multidimensional arrays (not supported)
- Struct and union types delegate member cleanup to ECPGfree_struct_member
- The function generates internal errors for multidimensional arrays and unknown complex datatypes
- Error messages include references to PACKAGE_BUGREPORT for user reporting of unknown datatypes
- Memory is freed in a specific order: first complex substructures, then the main ECPGtype structure
- Part of ECPG's comprehensive memory management system for type definitions