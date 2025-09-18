# JsonbToJsonbValue

## Location
src/backend/utils/adt/jsonb_util.c: 72 - 91

## Overview
Converts a Jsonb structure to a JsonbValue by wrapping the binary data for internal processing within the JSONB system.

## Definition


## Detailed Description
This function is a utility that converts a Jsonb structure (the on-disk/wire format) into a JsonbValue structure (the in-memory format used for processing). It specifically creates a binary JsonbValue that references the raw data within the Jsonb structure. The function sets up the JsonbValue to point directly to the root data of the Jsonb, making it accessible for further processing by other JSONB functions.

The conversion is lightweight as it doesn't parse or copy the data - it simply wraps the existing binary data in a JsonbValue container with the appropriate type marker (jbvBinary).

## Parameters / Member Variables
- : Input Jsonb structure containing the binary JSONB data
- : Output JsonbValue structure that will be populated to reference the Jsonb data

## Dependencies
- Functions called/Symbols referenced:
  - VARSIZE (macro for getting variable-length data size)
  - jbvBinary (JsonbValue type constant)
  - Jsonb (structure type)
- Called from (representative examples):
  - jsonb_subscript_assign
  - jsonb_set 
  - jsonb_insert
  - PG_RETURN_JSONB_P

## Notes and Other Information
- This is a foundational conversion function used throughout the JSONB subsystem
- The function performs no validation or parsing - it assumes the input Jsonb is valid
- The resulting JsonbValue references the original data, so the Jsonb structure must remain valid while the JsonbValue is in use
- Located in src/backend/utils/adt/jsonb_util.c:72-91