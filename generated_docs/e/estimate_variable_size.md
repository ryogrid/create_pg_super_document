# estimate_variable_size

## Location
src/backend/utils/misc/guc.c: 5856 - 5955

## Overview
Computes the space needed for dumping a given GUC variable during parallel worker serialization, providing size estimates for different GUC data types.

## Definition
```c
static Size estimate_variable_size(struct config_generic *gconf)
```

## Detailed Description
This function calculates the buffer space required to serialize a single GUC (Grand Unified Configuration) variable for transmission from leader to worker processes in parallel query execution. It handles different GUC data types (boolean, integer, real, string, enum) and accounts for all components that need to be serialized including the variable name, value, source file information, and metadata.

The function is designed to overestimate rather than underestimate space requirements to ensure buffer allocation is sufficient. For skippable GUCs (as determined by can_skip_gucvar), it returns zero space since these variables don't need to be transmitted.

For each GUC type, it calculates:
- Name length plus null terminator
- Maximum possible value length based on data type
- Source file path length (if present)  
- Metadata fields (source line, source type, context, role)

## Parameters / Member Variables
- `gconf`: Pointer to a config_generic structure representing the GUC variable to estimate size for

## Dependencies
- Functions called/Symbols referenced:
  - can_skip_gucvar
  - config_int, config_string, config_enum (struct types)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (enum constants)
  - config_enum_lookup_by_value
  - add_size
  - REALTYPE_PRECISION (constant)
- Called from (representative examples):
  - EstimateGUCStateSpace

## Notes and Other Information
- Returns 0 for skippable GUCs to optimize space usage
- Uses conservative estimates for integer values: 4 characters for values < 1000, 11 characters for larger values
- Real numbers are estimated using scientific notation format with REALTYPE_PRECISION digits
- NULL string values are transmitted as empty strings since GUC treats them equivalently  
- Enum values use the actual string length of the enum label
- Source file information is included only when a source file is specified
- All string values include space for null terminators
- Uses add_size() function for safe size arithmetic to prevent overflow