# _SPI_convert_params

## Location
[src/backend/executor/spi.c:2849-2873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L2849-L2873)

## Overview
_SPI_convert_params is a utility function that converts arrays of query parameters from SPI format into ParamListInfo structure used by the planner and executor.

## Definition
```c
static ParamListInfo _SPI_convert_params(int nargs, Oid *argtypes,
                                         Datum *Values, const char *Nulls)
```

## Detailed Description
The _SPI_convert_params function serves as a parameter conversion utility within the SPI framework. It transforms parameter data from the SPI's array-based format into the ParamListInfo structure that PostgreSQL's planner and executor subsystems expect.

The function creates a ParamListInfo structure and populates it with parameter values, types, and null indicators. Each parameter is marked with the PARAM_FLAG_CONST flag to indicate it represents a constant value. The function handles the special encoding used by SPI where null values are indicated by 'n' characters in the Nulls array.

This conversion is essential for bridging the gap between SPI's simplified parameter interface and the more complex parameter handling mechanisms used internally by PostgreSQL's query processing pipeline.

## Parameters / Member Variables
- `nargs`: Number of parameters in the arrays
- `argtypes`: Array of parameter type OIDs
- `Values`: Array of parameter values as Datum objects
- `Nulls`: Array of null indicators where 'n' indicates null value

## Dependencies
- Functions called/Symbols referenced:
  - [makeParamList](../m/makeParamList.md)
  - [ParamListInfo](../P/ParamListInfo.md)
  - ParamExternData  
  - PARAM_FLAG_CONST
- Called from (representative examples):
  - [SPI_execute_plan](SPI_execute_plan.md)
  - [SPI_execute_snapshot](SPI_execute_snapshot.md)
  - [SPI_execute_with_args](SPI_execute_with_args.md)
  - [SPI_cursor_open](SPI_cursor_open.md)
  - [SPI_cursor_open_with_args](SPI_cursor_open_with_args.md)

## Notes and Other Information
- Returns NULL if nargs is 0 (no parameters)
- Sets pflags to PARAM_FLAG_CONST for all parameters indicating constant values
- Uses SPI's null encoding convention where 'n' in Nulls array indicates null value
- Creates a complete ParamListInfo structure suitable for planner and executor consumption  
- Handles the impedance mismatch between SPI's array-based parameters and internal parameter structures
- Memory for the returned ParamListInfo is allocated in the current memory context