# GetTsmRoutine

## Location
[src/backend/access/tablesample/tablesample.c:27-40](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/tablesample/tablesample.c#L27-L40)

## Overview
GetTsmRoutine is a convenience function that retrieves a TsmRoutine struct by invoking a tablesample handler function and validates the returned structure.

## Definition

```c
struct",
			 tsmhandler);
```
## Detailed Description
GetTsmRoutine serves as a wrapper function that safely invokes a tablesample method handler function and validates its return value. The function takes an OID of a tablesample handler function and calls it with a NULL argument to retrieve the TsmRoutine structure. It performs error checking to ensure that the handler function returns a valid TsmRoutine struct, providing a consistent and safe way to obtain tablesample method routines throughout the PostgreSQL system.

The function is designed as a convenience routine specifically for error checking, ensuring that tablesample handler functions return properly formed TsmRoutine structures. This validation step is crucial for the tablesample infrastructure's reliability.

## Parameters / Member Variables
- : The OID of the tablesample handler function to be invoked. This function should return a TsmRoutine struct when called with a NULL argument.

## Dependencies
- Functions called/Symbols referenced:
  - OidFunctionCall1
  - [TsmRoutine](../T/TsmRoutine.md) (struct type)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - IsA (macro)
  - elog

- Called from (representative examples):
  - [ExecInitSampleScan](../E/ExecInitSampleScan.md) (src/backend/executor/nodeSamplescan.c:159)
  - [set_tablesample_rel_size](../s/set_tablesample_rel_size.md) (src/backend/optimizer/path/allpaths.c:832)
  - [set_tablesample_rel_pathlist](../s/set_tablesample_rel_pathlist.md) (src/backend/optimizer/path/allpaths.c:887)
  - [cost_samplescan](../c/cost_samplescan.md) (src/backend/optimizer/path/costsize.c:381)
  - [transformRangeTableSample](../t/transformRangeTableSample.md) (src/backend/parser/parse_clause.c:948)

## Notes and Other Information
- The function performs runtime type checking using IsA() macro to ensure the returned value is actually a TsmRoutine struct
- If the handler function returns NULL or an invalid structure, the function raises an ERROR with elog()
- This function is part of the tablesample API infrastructure in PostgreSQL
- The TsmRoutine structure contains function pointers and metadata needed for both planning and executing tablesample operations
- Located in src/backend/access/tablesample/tablesample.c:27-40

## Simplified Source

```c
TsmRoutine *
GetTsmRoutine(Oid tsmhandler)
{
    Datum datum;
    TsmRoutine *routine;

    // Call the tablesample handler function
    datum = OidFunctionCall1(tsmhandler, PointerGetDatum(NULL));
    routine = (TsmRoutine *) DatumGetPointer(datum);

    // Validate the returned routine structure
    if (routine == NULL || !IsA(routine, TsmRoutine))
        elog(ERROR, "tablesample handler function %u did not return a TsmRoutine struct",
             tsmhandler);

    return routine;
}
```