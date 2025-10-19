# init_real_reloption

## Location
[src/backend/access/common/reloptions.c:934-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L934-L953)

## Overview
A static function that allocates and initializes a new real (floating-point) reloption structure with specified configuration parameters and validation constraints.

## Definition
```c
static relopt_real *
init_real_reloption(bits32 kinds, const char *name, const char *desc,
                    double default_val, double min_val, double max_val,
                    LOCKMODE lockmode)
```

## Detailed Description
This function serves as an internal constructor for real (floating-point) type relation options in PostgreSQL. It creates a new `relopt_real` structure by first calling the generic `allocate_reloption` function to handle common initialization, then sets the real-specific properties including default value, minimum value, and maximum value constraints. The function is marked as static, indicating it's an internal helper function used within the reloptions subsystem. It mirrors the functionality of `init_int_reloption` but operates on double-precision floating-point values instead of integers.

## Parameters / Member Variables
- `kinds`: A bitmask specifying which relation kinds (table, index, etc.) this option applies to
- `name`: The name of the reloption as it appears in SQL
- `desc`: A human-readable description of the option for documentation/help
- `default_val`: The default double-precision floating-point value for this option
- `min_val`: The minimum allowed double value
- `max_val`: The maximum allowed double value
- `lockmode`: The lock mode required to change this option

## Dependencies
- Functions called/Symbols referenced:
  - [allocate_reloption](../a/allocate_reloption.md)
  - RELOPT_TYPE_REAL
- Called from (representative examples):
  - [add_real_reloption](../a/add_real_reloption.md)
  - [add_local_real_reloption](../a/add_local_real_reloption.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reloptions.c file
- The function follows PostgreSQL's pattern of separating allocation/initialization from registration
- The returned `relopt_real` structure contains both generic reloption fields and real-specific validation bounds
- Used internally by the public `add_real_reloption` and `add_local_real_reloption` functions
- Handles double-precision floating-point values, suitable for ratios, percentages, and other fractional parameters

## Simplified Source

```c
static relopt_real *
init_real_reloption(bits32 kinds, const char *name, const char *desc,
                    double default_val, double min_val, double max_val,
                    LOCKMODE lockmode)
{
    // Allocate a new real (floating-point) reloption structure
    relopt_real *newoption = (relopt_real *) allocate_reloption(kinds, RELOPT_TYPE_REAL,
                                                                name, desc, lockmode);

    // Set real-specific configuration values and constraints
    newoption->default_val = default_val;
    newoption->min = min_val;
    newoption->max = max_val;

    return newoption;
}
```