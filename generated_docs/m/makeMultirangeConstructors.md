# makeMultirangeConstructors

## Location
[src/backend/commands/typecmds.c:1811-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L1811-L1952)

## Overview
makeMultirangeConstructors creates three constructor functions for a newly defined multirange type, providing different ways to construct multirange values and enabling type casting functionality.

## Definition


## Detailed Description
This function creates three distinct constructor functions for multirange types to support different usage patterns:

1. **0-argument constructor** (): Creates empty multiranges
2. **1-argument constructor** (): Converts a single range to a multirange, primarily used for casting operations
3. **Variadic constructor** (): Accepts a variable number of ranges via a range array parameter

Each constructor uses the same name as the multirange type for intuitive usage. The 1-argument constructor is specifically designed to support casting from range types to multirange types, and its OID is returned via the  parameter for use in cast creation.

All constructors are created as internal language functions marked as immutable and parallel-safe, with DEPENDENCY_INTERNAL relationships to ensure automatic cleanup when the multirange type is dropped.

## Parameters / Member Variables
- : The name of the multirange type, used as the constructor function name
- : The namespace OID where the constructor functions should be created  
- : The OID of the multirange type that the constructors will return
- : The OID of the associated range type, used for the 1-argument constructor parameter
- : The OID of the range array type, used for the variadic constructor parameter
- : Output parameter that receives the OID of the 1-argument constructor for use in cast creation

## Dependencies
- Functions called/Symbols referenced:
  - [ProcedureCreate](../P/ProcedureCreate.md) (creates the constructor function catalog entries)
  - [buildoidvector](../b/buildoidvector.md) (constructs parameter type vectors)
  - [construct_array_builtin](../c/construct_array_builtin.md) (builds parameter mode arrays)
  - [recordDependencyOn](../r/recordDependencyOn.md) (establishes dependency relationships)
  - DEPENDENCY_INTERNAL (dependency type constant)
  - FUNC_PARAM_VARIADIC (parameter mode for variadic functions)
  - PROKIND_FUNCTION, PROVOLATILE_IMMUTABLE, PROPARALLEL_SAFE (function attribute constants)
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md) (during range type creation)
  - AlterTypeRecurseParams (during type alterations)

## Notes and Other Information
- Creates exactly 3 overloaded constructor functions with 0, 1, and variadic arguments
- The 1-argument constructor serves dual purposes: general construction and casting support
- Variadic constructor uses FUNC_PARAM_VARIADIC mode to accept variable arguments
- All constructors share the same name as the multirange type
- Functions are owned by the bootstrap superuser and marked strict (null inputs produce null output)
- DEPENDENCY_INTERNAL ensures pg_dump excludes these auto-generated constructors
- Returns the OID of the 1-argument constructor for subsequent cast creation