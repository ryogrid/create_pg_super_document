# add_local_int_reloption

## Location
[src/backend/access/common/reloptions.c:918-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L918-L933)

## Overview
A public function that creates and registers a new integer-type local relation option (reloption) with a specified memory offset for direct field access.

## Definition
```c
void
add_local_int_reloption(local_relopts *relopts, const char *name,
                        const char *desc, int default_val, int min_val,
                        int max_val, int offset)
```

## Detailed Description
This function provides the interface for adding integer-type local relation options to a specific local reloptions context. Unlike global reloptions registered via `add_int_reloption`, local reloptions are specific to particular access methods or extensions and are stored with a direct memory offset for efficient access. The function initializes the option with `RELOPT_KIND_LOCAL` and a lockmode of 0, then registers it with the provided local reloptions structure. The offset parameter allows the reloption value to be directly mapped to a field in a structure for fast runtime access.

## Parameters / Member Variables
- `relopts`: Pointer to the local reloptions structure where this option will be registered
- `name`: The name of the reloption as it will appear in SQL statements
- `desc`: A human-readable description of the option for documentation
- `default_val`: The default integer value used when the option is not explicitly specified
- `min_val`: The minimum allowed integer value for validation
- `max_val`: The maximum allowed integer value for validation
- `offset`: The byte offset within the target structure where this option's value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [init_int_reloption](../i/init_int_reloption.md)
  - [add_local_reloption](add_local_reloption.md)
  - RELOPT_KIND_LOCAL
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - [brin_minmax_multi_options](../b/brin_minmax_multi_options.md)
  - [gtsvector_options](../g/gtsvector_options.md)
  - Various access method and extension option setup functions

## Notes and Other Information
- Used primarily by access methods and extensions that need their own private set of reloptions
- The offset parameter enables direct memory mapping, eliminating the need for hash table lookups during runtime
- Always uses `RELOPT_KIND_LOCAL` and lockmode 0, as local options don't require locking like global ones
- Local reloptions are not visible in the global reloptions catalog and are scoped to their specific context
- Commonly used by BRIN indexes, GiST indexes, and other specialized access methods for performance-critical options

## Simplified Source

```c
void add_local_int_reloption(local_relopts *relopts, const char *name,
                             const char *desc, int default_val, int min_val,
                             int max_val, int offset) {
    // Create a local integer reloption with validation bounds
    relopt_int *new_option = init_int_reloption(RELOPT_KIND_LOCAL,
                                                name, desc, default_val,
                                                min_val, max_val, 0);

    // Add to the local reloptions structure with memory offset
    add_local_reloption(relopts, (relopt_gen *) new_option, offset);
}
```