# serialize_variable

## Location
[src/backend/utils/misc/guc.c:6032-6108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L6032-L6108)

## Overview
Serializes a single GUC (Grand Unified Configuration) variable into a binary format for storage or transmission, handling all PostgreSQL configuration parameter types.

## Definition

```c
static void
serialize_variable(char **destptr, Size *maxbytes,
				   struct config_generic *gconf)
```
## Detailed Description
The  function is a core component of PostgreSQL's configuration state serialization system. It takes a GUC variable and converts it into a serialized format that can be stored or transmitted. The function handles all supported GUC variable types (boolean, integer, real, string, and enum) and includes metadata such as the source file, line number, source context, and role information.

The function first checks if the GUC variable can be skipped using . For variables that need to be serialized, it writes the variable name followed by its value in a type-specific format. Boolean values are serialized as "true" or "false", integers as decimal strings, real numbers with precision, strings as-is (with NULL converted to empty string), and enums using their string representation.

Additionally, the function serializes metadata including the source file path, source line number (if applicable), source type, source context, and the role that set the variable.

## Parameters / Member Variables
- : Pointer to destination buffer pointer that gets updated as data is written
- : Pointer to remaining buffer size that gets decremented as data is written  
- : Generic configuration structure containing the GUC variable to serialize

## Dependencies
- Functions called/Symbols referenced:
  - [can_skip_gucvar](../c/can_skip_gucvar.md)
  - [do_serialize](../d/do_serialize.md)
  - [do_serialize_binary](../d/do_serialize_binary.md)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
- Configuration types:
  - [config_generic](../c/config_generic.md)
  - config_bool
  - [config_int](../c/config_int.md)
  - [config_real](../c/config_real.md)
  - [config_string](../c/config_string.md)
  - [config_enum](../c/config_enum.md)
- Constants:
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM
  - REALTYPE_PRECISION
- Called from:
  - [SerializeGUCState](../S/SerializeGUCState.md)

## Notes and Other Information
- This is a static function internal to the GUC serialization system
- Skippable GUC variables are filtered out to avoid serializing unnecessary configuration
- The function handles NULL string values by converting them to empty strings
- Source file and line number information is only serialized if a source file is specified
- The serialization format includes both text and binary components for efficient storage and parsing
- Part of PostgreSQL's mechanism for preserving configuration state across process boundaries