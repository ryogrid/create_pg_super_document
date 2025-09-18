# jsonb_populate_recordset

## Location
src/backend/utils/adt/jsonfuncs.c: 3972 - 3978

## Overview
A PostgreSQL SQL function that creates a set of records from a JSONB array, where each array element is a JSON object used to populate the fields of the target record type.

## Definition
```c
Datum jsonb_populate_recordset(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the JSONB variant of the json_populate_recordset functionality in PostgreSQL. It serves as a SQL-accessible function that converts a JSONB array of objects into a set of PostgreSQL records/tuples. The function is designed to process arrays where:

1. **Input structure**: The input must be a JSONB array containing JSON objects
2. **Record population**: Each object in the array is used to populate fields in a record of the specified target type
3. **Efficient processing**: Unlike json_populate_record which processes single objects, this function is optimized for batch processing by pushing tuple-building logic down into the semantic action handlers
4. **Per-object processing**: Each array element is processed individually, creating one output record per input object

The function acts as a thin wrapper around populate_recordset_worker, providing JSONB-specific parameters to handle the binary JSON format efficiently.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function call information structure containing:
  - The target record type definition
  - The JSONB array input to be processed
  - Additional function call metadata

## Dependencies
- Functions called/Symbols referenced:
  - [populate_recordset_worker](../p/populate_recordset_worker.md) (the core implementation function)
  - Datum (PostgreSQL data type for function returns)
  - PG_FUNCTION_ARGS (PostgreSQL function argument structure)
- Called from (representative examples):
  - SQL queries using jsonb_populate_recordset() function
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This is a SQL-callable function, exposed to users through PostgreSQL's function system
- The function name "jsonb_populate_recordset" is passed to populate_recordset_worker for error reporting
- Parameters (false, true) to populate_recordset_worker indicate JSONB-specific processing modes
- Requires input to be a JSONB array of objects - will error on other JSON structures
- More efficient than json_populate_recordset for large datasets due to JSONB's binary format
- Commonly used in ETL operations and data transformation workflows
- Each output record corresponds to one input array element, maintaining order
- Field mapping is based on JSON object key names matching record type field names