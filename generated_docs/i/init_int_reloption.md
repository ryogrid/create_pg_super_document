# init_int_reloption

## Location
[src/backend/access/common/reloptions.c:881-900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L881-L900)

## Overview
A static function that allocates and initializes a new integer reloption structure with specified configuration parameters and validation constraints.

## Definition

```c
static relopt_int *
init_int_reloption(bits32 kinds, const char *name, const char *desc,
				   int default_val, int min_val, int max_val,
				   LOCKMODE lockmode)
```
## Detailed Description
This function serves as an internal constructor for integer-type relation options (reloptions) in PostgreSQL. It creates a new  structure by first calling the generic  function to handle common initialization, then sets the integer-specific properties including default value, minimum value, and maximum value constraints. The function is marked as static, indicating it's an internal helper function used within the reloptions subsystem.

## Parameters / Member Variables
- `kinds`: A bitmask specifying which relation kinds (table, index, etc.) this option applies to
- `*name`: The name of the reloption as it appears in SQL
- `*desc`: A human-readable description of the option for documentation/help
- `default_val`: The default integer value for this option
- `min_val`: The minimum allowed integer value
- `max_val`: The maximum allowed integer value
- `lockmode`: The lock mode required to change this option
## Dependencies
- Functions called/Symbols referenced:
  - [allocate_reloption](../a/allocate_reloption.md)
  - RELOPT_TYPE_INT
- Called from (representative examples):
  - [add_int_reloption](../a/add_int_reloption.md)
  - [add_local_int_reloption](../a/add_local_int_reloption.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reloptions.c file
- The function follows PostgreSQL's pattern of separating allocation/initialization from registration
- The returned  structure contains both generic reloption fields and integer-specific validation bounds
- Used internally by the public  and  functions

## Simplified Source

```c
static relopt_int *
init_int_reloption(bits32 kinds, const char *name, const char *desc,
                   int default_val, int min_val, int max_val,
                   LOCKMODE lockmode)
{
    // Allocate a new integer reloption structure
    relopt_int *newoption = (relopt_int *) allocate_reloption(kinds, RELOPT_TYPE_INT,
                                                              name, desc, lockmode);

    // Set integer-specific configuration values
    newoption->default_val = default_val;
    newoption->min = min_val;
    newoption->max = max_val;

    return newoption;
}
```