# makeParamList

## Location
src/backend/nodes/params.c: 44 - 77

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
  - palloc (PostgreSQL memory allocation function)
  - paramlist_parser_setup (default parser setup function)
  - ParamListInfoData (struct type for parameter list data)
  - ParamExternData (struct type for individual parameter data)
- Called from (representative examples):
  - EvaluateParams (in prepare.c)
  - postquel_sub_params (in functions.c)
  - _SPI_convert_params (in spi.c)
  - copyParamList (in params.c)
  - RestoreParamList (in params.c)
  - exec_bind_message (in postgres.c)

## Notes and Other Information
- The function automatically sets up a default parserSetup function, though callers may override it if needed
- Most use-cases for ParamListInfos will never use the parserSetup function
- The allocated structure includes space for both the header (ParamListInfoData) and an array of parameter data (ParamExternData)
- All hook functions (paramFetch, paramCompile) are initially set to NULL
- The function is located in src/backend/nodes/params.c at lines 44-77