# get_object_field_start

## Location
src/backend/utils/adt/jsonfuncs.c: 1195 - 1241

## Overview
A JSON semantic action callback function that handles the start of object field processing, determining whether a field matches the target path and should be extracted.

## Definition
static JsonParseErrorType get_object_field_start(void *state, char *fname, bool isnull)

## Detailed Description
The `get_object_field_start` function is a semantic action callback used during JSON parsing to handle the beginning of object field processing. It compares the current field name against the target path names to determine if this field should be extracted. The function manages path navigation by checking if the current lexical level matches the expected path depth and if the field name matches the corresponding entry in the path_names array. When a target field is found, it prepares for value extraction by clearing previous results and setting up the appropriate extraction mode based on whether result normalization is required.

## Parameters / Member Variables
- `state`: A void pointer to the GetState structure containing parsing context and state information
- `fname`: The name of the current object field being processed
- `isnull`: Boolean indicating if the field name is null (currently unused in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [GetState](../G/GetState.md)
  - JSON_TOKEN_STRING
  - JSON_SUCCESS
  - JsonParseErrorType
- Called from (representative examples):
  - [get_worker](get_worker.md)

## Notes and Other Information
- This function is static and internal to jsonfuncs.c
- It performs path matching by comparing field names at the current lexical level with the target path
- The function handles both intermediate path navigation (setting pathok for deeper levels) and final target identification
- When a target field is found, it clears any previous results to handle object overrides
- The normalization behavior differs based on token type: for string tokens in normalize mode, it delegates to get_scalar; otherwise it records the starting position
- The function supports nested object navigation by maintaining path state across multiple lexical levels
- Always returns JSON_SUCCESS regardless of whether a match is found