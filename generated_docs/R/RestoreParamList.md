# RestoreParamList

## Location
[src/backend/nodes/params.c:292-334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L292-L334)

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
  - [ParamListInfo](../P/ParamListInfo.md) (return type)
  - [makeParamList](../m/makeParamList.md) (to create the parameter list structure)
  - ParamExternData (structure type for individual parameters)
  - [datumRestore](../d/datumRestore.md) (to deserialize individual datum values)
- Called from (representative examples):
  - [ExecParallelGetQueryDesc](../E/ExecParallelGetQueryDesc.md) (for parallel query execution in worker processes)

## Notes and Other Information
- Creates a static copy without dynamic parameter hooks
- Memory is allocated in CurrentMemoryContext
- Must be used with data created by SerializeParamList
- The start_address pointer is advanced past the consumed data
- Suitable for cross-process parameter transmission in parallel execution

## Simplified Source

```c
ParamListInfo RestoreParamList(char **start_address) {
    // Read number of parameters from serialized data
    int nparams;
    memcpy(&nparams, *start_address, sizeof(int));
    *start_address += sizeof(int);

    // Create parameter list structure
    ParamListInfo paramLI = makeParamList(nparams);

    // Restore each parameter
    for (int i = 0; i < nparams; i++) {
        ParamExternData *prm = &paramLI->params[i];

        // Read parameter type OID
        memcpy(&prm->ptype, *start_address, sizeof(Oid));
        *start_address += sizeof(Oid);

        // Read parameter flags
        memcpy(&prm->pflags, *start_address, sizeof(uint16));
        *start_address += sizeof(uint16);

        // Restore parameter value and null flag
        prm->value = datumRestore(start_address, &prm->isnull);
    }

    return paramLI;
}
```