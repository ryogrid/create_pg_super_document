# associate

## Location
[src/timezone/zic.c:1158-1242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1158-L1242)

## Overview
Associates timezone rules with timezone zones in the PostgreSQL timezone compilation system, establishing the relationships between rule sets and their corresponding zones.

## Definition
```c
static void associate(void)
```

## Detailed Description
The `associate` function is a core component of the timezone compilation process that establishes relationships between timezone rules and zones. It performs several critical operations:

1. **Rule Sorting and Validation**: Sorts all rules by name using the `rcomp` comparison function and validates that rules with the same name don't appear in multiple files (except when they're in the same file).

2. **Rule-Zone Association**: Groups rules by name and associates each group with the corresponding zones that reference those rule names.

3. **Orphaned Zone Handling**: For zones that don't have associated rules, it attempts to parse the rule field as a local standard time offset using `getsave` and validates format specifiers.

4. **Error Checking**: Performs comprehensive validation and exits with failure if any errors are encountered during the association process.

This function is essential for building the final timezone database structure that PostgreSQL uses for timezone conversions.

## Parameters / Member Variables
No parameters - operates on global data structures:
- `rules[]`: Global array of timezone rules
- `zones[]`: Global array of timezone zones
- `nrules`: Number of rules in the rules array
- `nzones`: Number of zones in the zones array

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard C library sorting function)
  - [rcomp](../r/rcomp.md) (comparison function for sorting rules)
  - [eat](../e/eat.md) (error reporting context function)
  - [warning](../w/warning.md) (warning message function)
  - [getsave](../g/getsave.md) (time offset parsing function)
  - EXIT_FAILURE (standard exit code constant)
- Called from (representative examples):
  - [main](../m/main.md)

## Notes and Other Information
- This is a static function with internal linkage, only accessible within src/timezone/zic.c
- The function modifies global state by setting z_rules and z_nrules fields in zone structures
- Validates that rules with identical names don't span multiple source files (except duplicates within the same file)
- Handles the special case where a zone's "rule" is actually a fixed offset rather than a rule name
- Performs format string validation for zones without rules, specifically checking for '%s' specifiers which require rule names
- Will terminate the program with EXIT_FAILURE if any validation errors are encountered
- Critical for the integrity of the timezone database compilation process

## Simplified Source

```c
// Simplified version of associate
static void associate(void) {
    // Phase 1: Sort and validate rules
    if (nrules != 0) {
        qsort(rules, nrules, sizeof *rules, rcomp);

        // Check for duplicate rule names across different files
        for (i = 0; i < nrules - 1; ++i) {
            if (strcmp(rules[i].r_name, rules[i + 1].r_name) == 0 &&
                strcmp(rules[i].r_filename, rules[i + 1].r_filename) != 0) {
                // Report warning for same rule name in multiple files
                warning_duplicate_rule_names();
                // Skip to next different rule name
                skip_to_next_rule_name();
            }
        }
    }

    // Phase 2: Initialize all zones (clear rule associations)
    for (i = 0; i < nzones; ++i) {
        zones[i].z_rules = NULL;
        zones[i].z_nrules = 0;
    }

    // Phase 3: Associate rule groups with matching zones
    for (base = 0; base < nrules; base = out) {
        // Find all rules with the same name
        rp = &rules[base];
        for (out = base + 1; out < nrules; ++out) {
            if (strcmp(rp->r_name, rules[out].r_name) != 0)
                break;
        }

        // Associate this rule group with matching zones
        for (i = 0; i < nzones; ++i) {
            if (strcmp(zones[i].z_rule, rp->r_name) == 0) {
                zones[i].z_rules = rp;
                zones[i].z_nrules = out - base;
            }
        }
    }

    // Phase 4: Handle zones without rules (fixed offsets)
    for (i = 0; i < nzones; ++i) {
        if (zones[i].z_nrules == 0) {
            // Parse as local standard time offset
            zones[i].z_save = getsave(zones[i].z_rule, &zones[i].z_isdst);

            // Validate format specifier
            if (zones[i].z_format_specifier == 's') {
                error("%%s in ruleless zone");
            }
        }
    }

    // Phase 5: Exit if any errors occurred
    if (errors) {
        exit(EXIT_FAILURE);
    }
}
```

Key simplifications made:
- Condensed the duplicate rule name checking logic into a conceptual block
- Simplified the nested loops for readability while preserving the algorithm
- Added clear phase comments to show the function's main steps
- Abstracted some repetitive error handling into conceptual function calls
- Focused on the core logic flow: sort, validate, associate, handle orphans, exit on errors