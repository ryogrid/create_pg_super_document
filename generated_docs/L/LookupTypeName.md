# LookupTypeName

## Location
[src/backend/parser/parse_type.c:38-72](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L38-L72)

## Overview
LookupTypeName is a wrapper function that provides a simplified interface for the typical case of looking up a PostgreSQL type by name.

## Definition

```c
Type
LookupTypeName(ParseState *pstate, const TypeName *typeName,
			   int32 *typmod_p, bool missing_ok)
```
## Detailed Description
LookupTypeName serves as a convenience wrapper around LookupTypeNameExtended, providing the most commonly used interface for type name resolution in PostgreSQL's parser. It delegates to LookupTypeNameExtended with the 'temp_ok' parameter set to true, allowing temporary types to be found during the lookup process. This function is part of the type resolution system that converts textual type names into internal Type structures during SQL parsing.

## Parameters / Member Variables
- : ParseState pointer containing the current parsing context and state information
- : TypeName structure containing the type name to be looked up, including schema qualification if present
- : Pointer to int32 where the type modifier will be stored (output parameter)
- : Boolean flag indicating whether to raise an error if the type is not found (false) or return NULL silently (true)

## Dependencies
- Functions called/Symbols referenced:
  - [LookupTypeNameExtended](LookupTypeNameExtended.md)
- Called from (representative examples):
  - [get_object_address_type](../g/get_object_address_type.md)
  - [compute_return_type](../c/compute_return_type.md)
  - [interpret_function_parameter_list](../i/interpret_function_parameter_list.md)
  - [AlterTypeOwner](../A/AlterTypeOwner.md)
  - [LookupTypeNameOid](LookupTypeNameOid.md)
  - [typenameType](../t/typenameType.md)
  - [parseTypeString](../p/parseTypeString.md)

## Notes and Other Information
This function is defined in src/backend/parser/parse_type.c:38-42 and serves as the standard entry point for type name lookups in most PostgreSQL parsing scenarios. It always allows temporary types (temp_ok=true) which is the typical behavior desired in most parsing contexts. For cases requiring more control over the lookup process, callers should use LookupTypeNameExtended directly.