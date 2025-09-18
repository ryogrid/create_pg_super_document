# ParamListInfo

## Location
src/include/nodes/params.h: 98 - 109

## Overview
ParamListInfo is a pointer typedef to ParamListInfoData struct that provides a framework for managing query parameters in PostgreSQL, supporting both static parameter arrays and dynamic parameter fetching through hook functions.

## Definition


## Detailed Description
ParamListInfo serves as the primary interface for parameter management in PostgreSQL's query execution system. It abstracts parameter handling by providing both direct parameter storage and callback mechanisms for dynamic parameter resolution. This design allows for flexible parameter management across different execution contexts, from prepared statements to stored procedures and parallel query execution.

The structure supports three main operational modes:
1. Static parameter arrays where all parameters are pre-populated
2. Dynamic parameter fetching via paramFetch hook for on-demand parameter resolution
3. Hybrid approaches combining both static and dynamic parameters

## Parameters / Member Variables
Since ParamListInfo is a typedef pointer to ParamListInfoData, the actual members are:
- : Hook function for dynamic parameter fetching
- : Argument passed to the parameter fetch hook
- : Hook function for parameter compilation optimization
- : Argument passed to the parameter compile hook
- : Hook function for parser setup with parameters
- : Argument passed to the parser setup hook
- : String representation of parameters for error reporting
- : Maximum number of parameters represented
- : Flexible array of ParamExternData for actual parameter storage

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfoData
  - ParamExternData
  - ParamFetchHook
  - ParamCompileHook
  - ParserSetupHook
  - Param
  - ExprState

- Called from (representative examples):
  - ExecuteQuery (prepared statements)
  - ExplainQuery (query explanation)
  - SPI_execute_plan_with_paramlist (SPI interface)
  - CreateQueryDesc (query descriptor creation)
  - BuildCachedPlan (plan caching)
  - ExecInitExprWithParams (expression initialization)

## Notes and Other Information
- Used extensively throughout PostgreSQL's query execution pipeline
- Critical for prepared statement parameter binding
- Supports both synchronous and asynchronous parameter resolution
- The paramFetch hook enables lazy parameter evaluation for performance optimization
- Parameter compilation hooks allow for JIT-style parameter optimization
- Essential for SPI (Server Programming Interface) parameter passing
- Used in parallel query execution for parameter serialization/deserialization