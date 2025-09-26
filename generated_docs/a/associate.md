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