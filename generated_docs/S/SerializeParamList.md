# SerializeParamList

## Location
[src/backend/nodes/params.c:229-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L229-L291)

## Overview
Serializes a ParamListInfo structure into caller-provided storage for cross-process parameter transmission, particularly in parallel query execution.

## Definition
void SerializeParamList(ParamListInfo paramLI, char **start_address)

## Detailed Description
This function converts a ParamListInfo structure into a serialized binary format that can be transmitted across process boundaries or stored persistently. The serialization format consists of:
1. A 4-byte integer containing the number of parameters
2. For each parameter: 4-byte type OID, 2 bytes of flags, and the serialized datum value

The function handles dynamic parameters by calling the paramFetch hook if present, ensuring all parameter values are properly materialized before serialization. The caller must ensure sufficient storage space is allocated (use EstimateParamListSpace to calculate required space).

## Parameters / Member Variables
- : The ParamListInfo structure to serialize (can be NULL)
- : Pointer to the storage location, updated to point past the written data

## Dependencies
- Functions called/Symbols referenced:
  - [ParamListInfo](../P/ParamListInfo.md) (structure type)
  - ParamExternData (structure type)
  - [get_typlenbyval](../g/get_typlenbyval.md) (to determine type properties)
  - [datumSerialize](../d/datumSerialize.md) (to serialize individual datum values)
- Called from (representative examples):
  - [ExecInitParallelPlan](../E/ExecInitParallelPlan.md) (for parallel query execution setup)

## Notes and Other Information
- The serialized format is self-contained and can be restored using RestoreParamList
- Does not include paramValuesStr in the serialization
- Handles NULL paramLI by writing zero parameters
- Works with both static and dynamic parameter lists by materializing dynamic parameters