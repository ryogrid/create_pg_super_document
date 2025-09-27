# ShowGUCOption

## Location
[src/backend/utils/misc/guc.c:5473-5593](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5473-L5593)

## Overview
ShowGUCOption retrieves the string representation of a PostgreSQL configuration variable (GUC) value, with support for unit conversion and custom display hooks.

## Definition

```c
char *
ShowGUCOption(struct config_generic *record, bool use_units)
```
## Detailed Description
ShowGUCOption is a core function in PostgreSQL's configuration system that converts configuration variable values to their string representation for display purposes. The function handles all supported GUC variable types (boolean, integer, real, string, and enum) and provides flexible formatting options.

Key features:
- Supports all PostgreSQL GUC variable types (PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM)
- Handles custom display hooks for specialized formatting
- Performs unit conversion for numeric values when requested
- Returns a palloc'd string that must be freed by the caller
- Uses appropriate number formatting for integers (INT64_FORMAT) and reals (%g)

The function processes each variable type differently:
- **Boolean**: Returns "on"/"off" or calls custom show_hook
- **Integer**: Formats as int64 with optional unit conversion
- **Real**: Formats as double with optional unit conversion  
- **String**: Returns the string value or empty string if null
- **Enum**: Looks up the enum value name or calls custom show_hook

## Parameters / Member Variables
- : Pointer to the config_generic structure representing the GUC variable
- : Boolean flag indicating whether to apply unit conversion for numeric values

## Dependencies
- Functions called/Symbols referenced:
  - [convert_int_from_base_unit](../c/convert_int_from_base_unit.md)
  - [convert_real_from_base_unit](../c/convert_real_from_base_unit.md)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - [pstrdup](../p/pstrdup.md)
  - snprintf
- Called from (representative examples):
  - [ReportGUCOption](../R/ReportGUCOption.md)
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
  - [ShowAllGUCConfig](ShowAllGUCConfig.md)
  - [GetConfigOptionValues](../G/GetConfigOptionValues.md)

## Notes and Other Information
- The function uses a local buffer of 256 characters for numeric formatting
- Unit conversion is only applied when use_units is true and the value is positive
- Custom show_hook functions take precedence over default formatting
- The returned string is allocated using palloc and must be freed by the caller
- For unknown variable types, returns "???" as a fallback
- Part of PostgreSQL's Grand Unified Configuration (GUC) system located in src/backend/utils/misc/guc.c

## Simplified Source

```c
// Simplified version of ShowGUCOption
char *ShowGUCOption(struct config_generic *record, bool use_units) {
    char buffer[256];
    const char *val;

    // Handle each GUC variable type differently
    switch (record->vartype) {
        case PGC_BOOL:
            {
                struct config_bool *conf = (struct config_bool *) record;

                // Use custom hook if available, otherwise "on"/"off"
                if (conf->show_hook) {
                    val = conf->show_hook();
                } else {
                    val = *conf->variable ? "on" : "off";
                }
            }
            break;

        case PGC_INT:
            {
                struct config_int *conf = (struct config_int *) record;

                if (conf->show_hook) {
                    val = conf->show_hook();
                } else {
                    // Format integer with optional unit conversion
                    int64 result = *conf->variable;
                    const char *unit;

                    if (use_units && result > 0 && (record->flags & GUC_UNIT)) {
                        convert_int_from_base_unit(result, record->flags & GUC_UNIT,
                                                 &result, &unit);
                    } else {
                        unit = "";
                    }

                    snprintf(buffer, sizeof(buffer), INT64_FORMAT "%s", result, unit);
                    val = buffer;
                }
            }
            break;

        case PGC_REAL:
            {
                struct config_real *conf = (struct config_real *) record;

                if (conf->show_hook) {
                    val = conf->show_hook();
                } else {
                    // Format real number with optional unit conversion
                    double result = *conf->variable;
                    const char *unit;

                    if (use_units && result > 0 && (record->flags & GUC_UNIT)) {
                        convert_real_from_base_unit(result, record->flags & GUC_UNIT,
                                                  &result, &unit);
                    } else {
                        unit = "";
                    }

                    snprintf(buffer, sizeof(buffer), "%g%s", result, unit);
                    val = buffer;
                }
            }
            break;

        case PGC_STRING:
            {
                struct config_string *conf = (struct config_string *) record;

                // Use custom hook, actual value, or empty string
                if (conf->show_hook) {
                    val = conf->show_hook();
                } else if (*conf->variable && **conf->variable) {
                    val = *conf->variable;
                } else {
                    val = "";
                }
            }
            break;

        case PGC_ENUM:
            {
                struct config_enum *conf = (struct config_enum *) record;

                // Use custom hook or lookup enum value name
                if (conf->show_hook) {
                    val = conf->show_hook();
                } else {
                    val = config_enum_lookup_by_value(conf, *conf->variable);
                }
            }
            break;

        default:
            // Fallback for unknown types
            val = "???";
            break;
    }

    // Return a palloc'd copy of the result string
    return pstrdup(val);
}
```

Key simplifications made:
- Added descriptive comments for each variable type
- Clarified the purpose of custom show_hook functions
- Organized unit conversion logic with clear comments
- Preserved all essential formatting and conversion functionality
- Emphasized the fallback mechanisms for each type
- Maintained proper memory management with pstrdup()
- Highlighted the comprehensive type support in PostgreSQL's GUC system