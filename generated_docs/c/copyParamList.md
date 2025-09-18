# copyParamList

## Location
[src/backend/nodes/params.c:78-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L78-L119)

## Overview
Creates a static, self-contained copy of a ParamListInfo structure, forcibly instantiating all parameter values and copying their datum values.

## Definition


## Detailed Description
The copyParamList function creates a deep copy of a ParamListInfo structure, with the specific intent of producing a static, self-contained set of parameter values. Unlike a simple copy, this function deliberately does not copy dynamic parameter hooks (paramFetch, paramCompile). Instead, it forcibly instantiates all available parameter values by calling the paramFetch hook if present, then performs deep copies of the actual datum values. For pass-by-reference datatypes, it uses datumCopy to ensure the copied values are independent of the original. The paramValuesStr field is intentionally not copied. The result is allocated in CurrentMemoryContext.

## Parameters / Member Variables
- : The source ParamListInfo structure to copy from. Returns NULL if this parameter is NULL or has numParams <= 0.

## Dependencies
- Functions called/Symbols referenced:
  - [makeParamList](../m/makeParamList.md) (creates the new parameter list structure)
  - [get_typlenbyval](../g/get_typlenbyval.md) (gets type length and pass-by-value information)
  - [datumCopy](../d/datumCopy.md) (performs deep copy of datum values for pass-by-reference types)
  - OidIsValid (macro to check if OID is valid)
  - ParamExternData (struct type for parameter data)
- Called from (representative examples):
  - [PerformCursorOpen](../P/PerformCursorOpen.md) (in portalcmds.c)
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) (in spi.c)

## Notes and Other Information
- The function intentionally creates static copies and does not preserve dynamic parameter hooks
- Returns NULL if the source parameter list is NULL or has no parameters
- Performs deep copying for pass-by-reference datatypes to ensure data independence
- The paramValuesStr field from the source is not copied to the destination
- All parameter values are forcibly instantiated, even if they were originally dynamic
- The function is located in src/backend/nodes/params.c at lines 78-119