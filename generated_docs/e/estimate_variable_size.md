# estimate_variable_size

## Location
[src/backend/utils/misc/guc.c:5856-5955](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5856-L5955)

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
  - [can_skip_gucvar](../c/can_skip_gucvar.md)
  - [config_int](../c/config_int.md), config_string, config_enum (struct types)
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM (enum constants)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - [add_size](../a/add_size.md)
  - REALTYPE_PRECISION (constant)
- Called from (representative examples):
  - [EstimateGUCStateSpace](../E/EstimateGUCStateSpace.md)

## Notes and Other Information
- Returns 0 for skippable GUCs to optimize space usage
- Uses conservative estimates for integer values: 4 characters for values < 1000, 11 characters for larger values
- Real numbers are estimated using scientific notation format with REALTYPE_PRECISION digits
- NULL string values are transmitted as empty strings since GUC treats them equivalently  
- Enum values use the actual string length of the enum label
- Source file information is included only when a source file is specified
- All string values include space for null terminators
- Uses add_size() function for safe size arithmetic to prevent overflow

## Simplified Source

```c
static Size estimate_variable_size(struct config_generic *gconf) {
    Size size;
    Size valsize = 0;

    // Skip variables that don't need serialization
    if (can_skip_gucvar(gconf))
        return 0;

    // Start with name length + null terminator
    size = strlen(gconf->name) + 1;

    // Calculate maximum value size based on type
    switch (gconf->vartype) {
        case PGC_BOOL:
            valsize = 5;  // max(strlen('true'), strlen('false'))
            break;
        case PGC_INT:
            {
                struct config_int *conf = (struct config_int *) gconf;
                // Use optimized size for small values, max for larger ones
                valsize = abs(*conf->variable) < 1000 ? 4 : 11;
            }
            break;
        case PGC_REAL:
            // Scientific notation: sign + digit + decimal + precision + exponent
            valsize = 1 + 1 + 1 + REALTYPE_PRECISION + 5;
            break;
        case PGC_STRING:
            {
                struct config_string *conf = (struct config_string *) gconf;
                valsize = *conf->variable ? strlen(*conf->variable) : 0;
            }
            break;
        case PGC_ENUM:
            {
                struct config_enum *conf = (struct config_enum *) gconf;
                valsize = strlen(config_enum_lookup_by_value(conf, *conf->variable));
            }
            break;
    }

    // Add space for value + terminator + source file + terminator
    size = add_size(size, valsize + 1);
    if (gconf->sourcefile)
        size = add_size(size, strlen(gconf->sourcefile));
    size = add_size(size, 1);

    // Add metadata sizes
    if (gconf->sourcefile && gconf->sourcefile[0])
        size = add_size(size, sizeof(gconf->sourceline));
    size = add_size(size, sizeof(gconf->source));
    size = add_size(size, sizeof(gconf->scontext));
    size = add_size(size, sizeof(gconf->srole));

    return size;
}
```