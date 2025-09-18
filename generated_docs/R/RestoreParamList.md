# RestoreParamList

## Location
src/backend/nodes/params.c: 292 - 334

## Overview
Recreates a ParamListInfo structure from serialized data created by SerializeParamList, producing a static, self-contained parameter list.

## Definition
ParamListInfo RestoreParamList(char **start_address)

## Detailed Description
This function deserializes parameter data that was previously serialized by SerializeParamList, reconstructing a complete ParamListInfo structure. The function reads the binary format sequentially:
1. Reads the number of parameters (4-byte integer)
2. For each parameter: reads type OID, flags, and deserializes the datum value

The resulting ParamListInfo is allocated in the current memory context and contains static, materialized parameter values without any dynamic parameter hooks. This makes it suitable for use in parallel worker processes or other contexts where the original parameter source is not available.

## Parameters / Member Variables
- : Pointer to the serialized data, updated to point past the read data

## Dependencies
- Functions called/Symbols referenced:
  - ParamListInfo (return type)
  - makeParamList (to create the parameter list structure)
  - ParamExternData (structure type for individual parameters)
  - datumRestore (to deserialize individual datum values)
- Called from (representative examples):
  - ExecParallelGetQueryDesc (for parallel query execution in worker processes)

## Notes and Other Information
- Creates a static copy without dynamic parameter hooks
- Memory is allocated in CurrentMemoryContext
- Must be used with data created by SerializeParamList
- The start_address pointer is advanced past the consumed data
- Suitable for cross-process parameter transmission in parallel execution