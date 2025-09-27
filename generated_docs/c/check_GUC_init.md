# check_GUC_init

## Location
[src/backend/utils/misc/guc.c:1438-1531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1438-L1531)

## Overview
Validates the initialization state of a GUC parameter by checking that C variables match their boot values and that flag combinations are consistent.

## Definition
```c
static bool check_GUC_init(struct config_generic *gconf)
```

## Detailed Description
This function performs comprehensive validation checks on GUC (Grand Unified Configuration) parameters during initialization to ensure they are in a consistent state. The function validates two main aspects: value consistency and flag combinations.

For value consistency, it checks that the current C variable value matches the expected boot value for each parameter type (BOOL, INT, REAL, STRING, ENUM). This helps detect cases where C variables have been inadvertently modified before GUC initialization is complete.

For flag combinations, it enforces specific rules about parameter visibility flags, particularly ensuring that parameters marked with GUC_NO_SHOW_ALL (hidden from SHOW ALL) also have GUC_NOT_IN_SAMPLE (excluded from postgresql.conf.sample), maintaining consistency in parameter exposure policies.

## Parameters / Member Variables
- `gconf`: Pointer to the generic GUC configuration structure to validate

## Dependencies
- Functions called/Symbols referenced:
  - [config_generic](config_generic.md): Base structure for all GUC parameters
  - PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM: Parameter type constants
  - config_bool, config_int, config_real, config_string, config_enum: Type-specific GUC structures
  - GUC_NO_SHOW_ALL, GUC_NOT_IN_SAMPLE: Flag constants for parameter visibility control
- Called from (representative examples):
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md): Main GUC initialization routine
  - [define_custom_variable](../d/define_custom_variable.md): Custom parameter definition function

## Notes and Other Information
- Returns false and logs issues when inconsistencies are detected
- Helps debug initialization problems by identifying parameters with unexpected values
- The function is static, used only within the GUC subsystem
- Critical for maintaining data integrity during PostgreSQL startup
- Validates both built-in and custom GUC parameters
- Located in src/backend/utils/misc/guc.c:1438-1531

## Simplified Source

```c
// Simplified version of check_GUC_init
static bool check_GUC_init(struct config_generic *gconf) {
    // Check that C variables match their boot values for each parameter type
    switch (gconf->vartype) {
        case PGC_BOOL: {
            struct config_bool *conf = (struct config_bool *) gconf;

            // Ensure boolean variable matches boot value
            if (*conf->variable && !conf->boot_val) {
                elog(LOG, "GUC (PGC_BOOL) %s mismatch: boot_val=%d, C-var=%d",
                     conf->gen.name, conf->boot_val, *conf->variable);
                return false;
            }
            break;
        }
        case PGC_INT: {
            struct config_int *conf = (struct config_int *) gconf;

            // Check integer variable consistency (non-zero values must match boot value)
            if (*conf->variable != 0 && *conf->variable != conf->boot_val) {
                elog(LOG, "GUC (PGC_INT) %s mismatch: boot_val=%d, C-var=%d",
                     conf->gen.name, conf->boot_val, *conf->variable);
                return false;
            }
            break;
        }
        case PGC_REAL: {
            struct config_real *conf = (struct config_real *) gconf;

            // Check real variable consistency (non-zero values must match boot value)
            if (*conf->variable != 0.0 && *conf->variable != conf->boot_val) {
                elog(LOG, "GUC (PGC_REAL) %s mismatch: boot_val=%g, C-var=%g",
                     conf->gen.name, conf->boot_val, *conf->variable);
                return false;
            }
            break;
        }
        case PGC_STRING: {
            struct config_string *conf = (struct config_string *) gconf;

            // Check string variable consistency (non-null values must match boot value)
            if (*conf->variable != NULL &&
                (conf->boot_val == NULL || strcmp(*conf->variable, conf->boot_val) != 0)) {
                elog(LOG, "GUC (PGC_STRING) %s mismatch: boot_val=%s, C-var=%s",
                     conf->gen.name,
                     conf->boot_val ? conf->boot_val : "<null>",
                     *conf->variable);
                return false;
            }
            break;
        }
        case PGC_ENUM: {
            struct config_enum *conf = (struct config_enum *) gconf;

            // Check enum variable matches boot value exactly
            if (*conf->variable != conf->boot_val) {
                elog(LOG, "GUC (PGC_ENUM) %s mismatch: boot_val=%d, C-var=%d",
                     conf->gen.name, conf->boot_val, *conf->variable);
                return false;
            }
            break;
        }
    }

    // Validate flag combinations for parameter visibility
    // GUC_NO_SHOW_ALL requires GUC_NOT_IN_SAMPLE for consistency
    if ((gconf->flags & GUC_NO_SHOW_ALL) &&
        !(gconf->flags & GUC_NOT_IN_SAMPLE)) {
        elog(LOG, "GUC %s invalid flags: NO_SHOW_ALL requires NOT_IN_SAMPLE",
             gconf->name);
        return false;
    }

    return true;
}
```

Key simplifications made:
- Added descriptive comments explaining each validation step
- Consolidated repetitive error logging patterns
- Clarified the logic for each parameter type check
- Enhanced readability of flag validation logic
- Maintained all essential validation logic and error reporting