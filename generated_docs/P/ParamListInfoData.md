# ParamListInfoData

## Location
[src/include/nodes/params.h:110-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/params.h#L110-L126)

## Overview
ParamListInfoData is the concrete struct definition that implements the parameter management framework in PostgreSQL, providing storage and hooks for both static and dynamic parameter handling in query execution.

## Definition


## Detailed Description
ParamListInfoData is the actual struct that stores parameter information and management hooks used throughout PostgreSQL's query execution system. It provides a flexible architecture that supports both pre-populated parameter arrays and dynamic parameter resolution through callback functions.

The structure is designed to handle various parameter scenarios:
- Prepared statements with known parameter values
- Dynamic parameter resolution for complex expressions
- Parameter compilation optimization for frequently executed queries
- Parser setup customization for different execution contexts

The flexible array member design allows for efficient memory allocation while supporting variable numbers of parameters.

## Parameters / Member Variables
- : Function pointer for dynamic parameter retrieval when parameters are not pre-populated
- : Context argument passed to the paramFetch hook function
- : Function pointer for parameter compilation optimization during expression evaluation
- : Context argument passed to the paramCompile hook function
- : Function pointer for customizing parser behavior with parameter context
- : Context argument passed to the parserSetup hook function
- : String representation of all parameters for error reporting and debugging
- : Total number of parameters that this structure can represent
- : Flexible array containing actual parameter data as ParamExternData structures

## Dependencies
- Functions called/Symbols referenced:
  - ParamExternData
  - FLEXIBLE_ARRAY_MEMBER
  - ParamFetchHook (typedef)
  - ParamCompileHook (typedef)
  - ParserSetupHook (typedef)

- Called from (representative examples):
  - [makeParamList](../m/makeParamList.md) (parameter list creation)
  - [ParamListInfo](ParamListInfo.md) (as the underlying type)

## Notes and Other Information
- The params[] array can be empty (length zero) if paramFetch hook is provided for dynamic parameter resolution
- When paramFetch is not used, params[] must contain exactly numParams elements
- Used as the foundation for all parameter passing in PostgreSQL's execution engine
- Critical for memory-efficient parameter management in high-performance query execution
- The hook-based design enables extensibility for different parameter sources and optimization strategies