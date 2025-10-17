# GetConfigOptionValues

## Location
[src/backend/utils/misc/guc_funcs.c:594-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L594-L806)

## Overview
Extracts and formats field values for a configuration parameter to display in the pg_settings system view.

## Definition

```c
static void
GetConfigOptionValues(struct config_generic *conf, const char **values)
```
## Detailed Description
This function takes a configuration parameter structure and populates an array of string values representing all the attributes of that parameter as they should appear in the pg_settings system view. It handles the extraction of both generic attributes (name, setting, unit, group, description, etc.) and type-specific attributes (min_val, max_val, enumvals, boot_val, reset_val) for all supported PostgreSQL configuration parameter types (boolean, integer, real, string, and enum).

The function performs type-specific handling to format values appropriately:
- For boolean parameters: converts true/false to "on"/"off" strings
- For numeric parameters: formats min/max values and converts numbers to strings
- For string parameters: handles NULL values appropriately
- For enum parameters: builds a formatted list of valid options and looks up symbolic names
- For all types: manages source file information based on user privileges

## Parameters / Member Variables
- `*conf`: Pointer to the generic configuration parameter structure containing the parameter's metadata and current values
- `**values`: Output array of string pointers (17 elements) to be populated with formatted parameter information
## Dependencies
- Functions called/Symbols referenced:
  - [ShowGUCOption](../S/ShowGUCOption.md)
  - [get_config_unit_name](../g/get_config_unit_name.md)
  - [config_enum_get_options](../c/config_enum_get_options.md)
  - [config_enum_lookup_by_value](../c/config_enum_lookup_by_value.md)
  - [has_privs_of_role](../h/has_privs_of_role.md)
  - [GetUserId](GetUserId.md)
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [show_all_settings](../s/show_all_settings.md)

## Notes and Other Information
- This is a static function used internally by the GUC (Grand Unified Configuration) system
- The function populates exactly 17 string values corresponding to the columns in pg_settings
- Source file and line number information is only shown to users with appropriate privileges (ROLE_PG_READ_ALL_SETTINGS)
- The function handles all five PostgreSQL configuration parameter types: PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, and PGC_ENUM
- Memory allocation is performed using pstrdup() for string values that need to persist beyond the function call

## Simplified Source

```c
static void
GetConfigOptionValues(struct config_generic *conf, const char **values)
{
    char buffer[256];

    // Fill generic attributes (indexes 0-8)
    values[0] = conf->name;                           // name
    values[1] = ShowGUCOption(conf, false);          // current setting
    values[2] = get_config_unit_name(conf->flags);   // unit
    values[3] = _(config_group_names[conf->group]);  // group
    values[4] = conf->short_desc ? _(conf->short_desc) : NULL;  // short description
    values[5] = conf->long_desc ? _(conf->long_desc) : NULL;    // long description
    values[6] = GucContext_Names[conf->context];     // context
    values[7] = config_type_names[conf->vartype];    // variable type
    values[8] = GucSource_Names[conf->source];       // source

    // Fill type-specific attributes (indexes 9-13)
    switch (conf->vartype) {
        case PGC_BOOL: {
            struct config_bool *bool_conf = (struct config_bool *) conf;
            values[9] = values[10] = values[11] = NULL;  // min/max/enumvals unused
            values[12] = pstrdup(bool_conf->boot_val ? "on" : "off");
            values[13] = pstrdup(bool_conf->reset_val ? "on" : "off");
            break;
        }

        case PGC_INT: {
            struct config_int *int_conf = (struct config_int *) conf;
            snprintf(buffer, sizeof(buffer), "%d", int_conf->min);
            values[9] = pstrdup(buffer);   // min_val
            snprintf(buffer, sizeof(buffer), "%d", int_conf->max);
            values[10] = pstrdup(buffer);  // max_val
            values[11] = NULL;             // enumvals unused
            snprintf(buffer, sizeof(buffer), "%d", int_conf->boot_val);
            values[12] = pstrdup(buffer);  // boot_val
            snprintf(buffer, sizeof(buffer), "%d", int_conf->reset_val);
            values[13] = pstrdup(buffer);  // reset_val
            break;
        }

        case PGC_REAL: {
            struct config_real *real_conf = (struct config_real *) conf;
            snprintf(buffer, sizeof(buffer), "%g", real_conf->min);
            values[9] = pstrdup(buffer);   // min_val
            snprintf(buffer, sizeof(buffer), "%g", real_conf->max);
            values[10] = pstrdup(buffer);  // max_val
            values[11] = NULL;             // enumvals unused
            snprintf(buffer, sizeof(buffer), "%g", real_conf->boot_val);
            values[12] = pstrdup(buffer);  // boot_val
            snprintf(buffer, sizeof(buffer), "%g", real_conf->reset_val);
            values[13] = pstrdup(buffer);  // reset_val
            break;
        }

        case PGC_STRING: {
            struct config_string *str_conf = (struct config_string *) conf;
            values[9] = values[10] = values[11] = NULL;  // min/max/enumvals unused
            values[12] = str_conf->boot_val ? pstrdup(str_conf->boot_val) : NULL;
            values[13] = str_conf->reset_val ? pstrdup(str_conf->reset_val) : NULL;
            break;
        }

        case PGC_ENUM: {
            struct config_enum *enum_conf = (struct config_enum *) conf;
            values[9] = values[10] = NULL;  // min/max unused
            // Build enumerated options list
            values[11] = config_enum_get_options(enum_conf, "{\"", "\"}", "\",\"");
            values[12] = pstrdup(config_enum_lookup_by_value(enum_conf, enum_conf->boot_val));
            values[13] = pstrdup(config_enum_lookup_by_value(enum_conf, enum_conf->reset_val));
            break;
        }

        default:
            // Fallback: set all type-specific values to NULL
            values[9] = values[10] = values[11] = values[12] = values[13] = NULL;
            break;
    }

    // Fill source location info (indexes 14-15) - only for privileged users
    if (conf->source == PGC_S_FILE &&
        has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)) {
        values[14] = conf->sourcefile;
        snprintf(buffer, sizeof(buffer), "%d", conf->sourceline);
        values[15] = pstrdup(buffer);
    } else {
        values[14] = values[15] = NULL;
    }

    // Fill pending restart flag (index 16)
    values[16] = (conf->status & GUC_PENDING_RESTART) ? "t" : "f";
}
```