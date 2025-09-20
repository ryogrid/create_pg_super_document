# JsonbToJsonbValue

## Location
[src/backend/utils/adt/jsonb_util.c:72-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L72-L91)

## Overview
Converts a Jsonb structure to a JsonbValue by wrapping the binary data for internal processing within the JSONB system.

## Definition

```c
void
JsonbToJsonbValue(Jsonb *jsonb, JsonbValue *val)
```
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
  - [jsonb_subscript_assign](../j/jsonb_subscript_assign.md)
  - [jsonb_set](../j/jsonb_set.md) 
  - [jsonb_insert](../j/jsonb_insert.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- This is a foundational conversion function used throughout the JSONB subsystem
- The function performs no validation or parsing - it assumes the input Jsonb is valid
- The resulting JsonbValue references the original data, so the Jsonb structure must remain valid while the JsonbValue is in use
- Located in src/backend/utils/adt/jsonb_util.c:72-91