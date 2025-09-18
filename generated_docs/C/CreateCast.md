# CreateCast

## Location
[src/backend/commands/functioncmds.c:1521-1783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L1521-L1783)

## Overview
Implements the CREATE CAST command to define type conversion operations between source and target data types using various coercion methods.

## Definition


## Detailed Description
CreateCast processes CREATE CAST statements to establish type conversion mechanisms in PostgreSQL. The function performs extensive validation and supports three coercion methods:

1. **Function-based casts** - Use a conversion function with strict parameter validation
2. **Input/Output casts** - Convert via text representation using type I/O functions  
3. **Binary-compatible casts** - Direct memory reinterpretation (superuser only)

Key validation steps include:
- Permission checks requiring ownership or usage rights on both types
- Pseudo-type rejection for source and target types
- Domain type warnings (allowed but discouraged for compatibility)
- Cast function signature validation (1-3 parameters with specific types)
- Binary compatibility checks for physical type representation
- Restriction of binary casts for composite, array, range, enum, and domain types
- Coercion context mapping from SQL syntax to internal codes

The function delegates actual catalog insertion to CastCreate after completing all validation.

## Parameters / Member Variables
- : CreateCastStmt structure containing source type, target type, optional cast function, coercion context, and method specification

## Dependencies
- Functions called/Symbols referenced:
  - [typenameTypeId](../t/typenameTypeId.md)
  - [get_typtype](../g/get_typtype.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [LookupFuncWithArgs](../L/LookupFuncWithArgs.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [IsBinaryCoercibleWithCast](../I/IsBinaryCoercibleWithCast.md)
  - superuser
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - [get_element_type](../g/get_element_type.md)
  - [CastCreate](CastCreate.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility.c:1720)

## Notes and Other Information
- Enforces superuser requirement for binary-compatible casts due to crash risk from erroneous casts
- Validates cast function signatures: first parameter must match/be coercible from source type, optional second parameter must be integer, optional third parameter must be boolean
- Includes commented-out volatility check (#ifdef NOT_USED) that was disabled to maintain compatibility with user-defined types
- Prevents self-casts except for length coercion functions (multi-argument functions)
- Maps SQL coercion contexts (IMPLICIT, ASSIGNMENT, EXPLICIT) to internal character codes
- Physical compatibility checks ensure matching length, pass-by-value semantics, and alignment between types for binary casts