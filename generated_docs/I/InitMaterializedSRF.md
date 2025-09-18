# InitMaterializedSRF

## Location
src/backend/utils/fmgr/funcapi.c: 76 - 132

## Overview
InitMaterializedSRF is a helper function that initializes the state of a set-returning function (SRF) for use in materialize mode, setting up the necessary tuplestore and tuple descriptor structures.

## Definition
```c
void InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags)
```

## Detailed Description
This function serves as a standardized initialization routine for set-returning functions that need to operate in materialize mode. It performs several critical tasks:

1. **Validation**: Checks that the calling context supports returning a tuplestore and that materialize mode is allowed
2. **Memory Management**: Switches to the per-query memory context to ensure proper lifetime management of created structures
3. **Tuple Descriptor Setup**: Creates or copies the tuple descriptor that defines the structure of returned tuples
4. **Tuplestore Creation**: Initializes a tuplestore to hold the result set
5. **Configuration**: Sets up the ReturnSetInfo structure with the appropriate mode and result structures

The function includes comprehensive error handling to ensure that set-returning functions are only called in appropriate contexts.

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo structure containing function call context and parameters
- `flags`: Control flags that modify behavior:
  - `MAT_SRF_USE_EXPECTED_DESC`: Use the tuple descriptor from expectedDesc instead of deriving it
  - `MAT_SRF_BLESS`: Complete the tuple descriptor information (necessary for transient RECORD datatypes)

## Dependencies
- Functions called/Symbols referenced:
  - CreateTupleDescCopy
  - get_call_result_type
  - BlessTupleDesc
  - tuplestore_begin_heap
  - MemoryContextSwitchTo
- Types referenced:
  - ReturnSetInfo
  - TuplestoreState
  - bits32
  - FunctionCallInfo
- Constants used:
  - SFRM_Materialize
  - SFRM_Materialize_Random
  - TYPEFUNC_COMPOSITE
  - MAT_SRF_USE_EXPECTED_DESC
  - MAT_SRF_BLESS
- Called from (representative examples):
  - pg_available_extensions
  - pg_prepared_statement
  - pg_timezone_names
  - pg_stat_get_activity
  - text_to_table

## Notes and Other Information
- The function must be called in a context where the caller supports returning a tuplestore
- Memory allocation is done in the per-query memory context to ensure proper cleanup
- Random access capability is determined based on the allowed modes in ReturnSetInfo
- This is a foundational function used by many PostgreSQL system functions that return result sets
- Error conditions will raise exceptions rather than returning error codes
- The tuplestore created supports both sequential and random access depending on caller requirements