# makeParamList

## Location
[src/backend/nodes/params.c:44-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L44-L77)

## Overview
Allocates and initializes a new ParamListInfo structure for managing query parameters in PostgreSQL's parameter framework.

## Definition


## Detailed Description
The makeParamList function creates and initializes a new ParamListInfo structure with space for a specified number of parameters. It handles memory allocation for both the ParamListInfoData header and the array of ParamExternData entries. The function sets up default values for all fields, including a default parser setup function (paramlist_parser_setup). This function is designed to support both static parameter lists (with a fixed number of parameters) and dynamic parameter lists (by passing 0 for numParams and setting numParams manually later).

## Parameters / Member Variables
- : The number of parameters to allocate space for in the parameter list. Pass 0 for dynamic parameter lists that will have their size set manually later.

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (macro for struct field offset calculation)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - [paramlist_parser_setup](../p/paramlist_parser_setup.md) (default parser setup function)
  - [ParamListInfoData](../P/ParamListInfoData.md) (struct type for parameter list data)
  - ParamExternData (struct type for individual parameter data)
- Called from (representative examples):
  - [EvaluateParams](../E/EvaluateParams.md) (in prepare.c)
  - [postquel_sub_params](../p/postquel_sub_params.md) (in functions.c)
  - [_SPI_convert_params](../S/_SPI_convert_params.md) (in spi.c)
  - [copyParamList](../c/copyParamList.md) (in params.c)
  - [RestoreParamList](../R/RestoreParamList.md) (in params.c)
  - [exec_bind_message](../e/exec_bind_message.md) (in postgres.c)

## Notes and Other Information
- The function automatically sets up a default parserSetup function, though callers may override it if needed
- Most use-cases for ParamListInfos will never use the parserSetup function
- The allocated structure includes space for both the header (ParamListInfoData) and an array of parameter data (ParamExternData)
- All hook functions (paramFetch, paramCompile) are initially set to NULL
- The function is located in src/backend/nodes/params.c at lines 44-77