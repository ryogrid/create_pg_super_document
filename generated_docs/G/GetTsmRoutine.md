# GetTsmRoutine

## Location
src/backend/access/tablesample/tablesample.c: 27 - 40

## Overview
GetTsmRoutine is a convenience function that retrieves a TsmRoutine struct by invoking a tablesample handler function and validates the returned structure.

## Definition


## Detailed Description
GetTsmRoutine serves as a wrapper function that safely invokes a tablesample method handler function and validates its return value. The function takes an OID of a tablesample handler function and calls it with a NULL argument to retrieve the TsmRoutine structure. It performs error checking to ensure that the handler function returns a valid TsmRoutine struct, providing a consistent and safe way to obtain tablesample method routines throughout the PostgreSQL system.

The function is designed as a convenience routine specifically for error checking, ensuring that tablesample handler functions return properly formed TsmRoutine structures. This validation step is crucial for the tablesample infrastructure's reliability.

## Parameters / Member Variables
- : The OID of the tablesample handler function to be invoked. This function should return a TsmRoutine struct when called with a NULL argument.

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall1
  - TsmRoutine (struct type)
  - DatumGetPointer
  - PointerGetDatum
  - IsA (macro)
  - elog

- Called from (representative examples):
  - ExecInitSampleScan (src/backend/executor/nodeSamplescan.c:159)
  - set_tablesample_rel_size (src/backend/optimizer/path/allpaths.c:832)
  - set_tablesample_rel_pathlist (src/backend/optimizer/path/allpaths.c:887)
  - cost_samplescan (src/backend/optimizer/path/costsize.c:381)
  - transformRangeTableSample (src/backend/parser/parse_clause.c:948)

## Notes and Other Information
- The function performs runtime type checking using IsA() macro to ensure the returned value is actually a TsmRoutine struct
- If the handler function returns NULL or an invalid structure, the function raises an ERROR with elog()
- This function is part of the tablesample API infrastructure in PostgreSQL
- The TsmRoutine structure contains function pointers and metadata needed for both planning and executing tablesample operations
- Located in src/backend/access/tablesample/tablesample.c:27-40