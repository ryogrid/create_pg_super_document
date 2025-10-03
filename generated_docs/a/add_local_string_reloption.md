# add_local_string_reloption

## Location
[src/backend/access/common/reloptions.c:1118-1155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1118-L1155)

## Overview
Adds a new local string-type reloption to a specific local reloption set, used for defining string configuration parameters that are local to particular access methods or extensions.

## Definition

```c
struct_array_builtin(array, TEXTOID, &oldoptions, NULL, &noldoptions);
```
## Detailed Description
This function registers a local string reloption within a specific local reloption set (). Unlike global reloptions that are available system-wide, local reloptions are scoped to particular access methods, table access methods, or extensions. The function creates a string reloption with  kind and registers it using . The  parameter specifies where in the resulting bytea structure the string value offset should be stored.

## Parameters / Member Variables
- : Pointer to the local reloption set where this option will be added
- : The name of the reloption as it appears in SQL
- : Human-readable description of the option's purpose
- : Default string value if not specified by users
- : Optional function to validate string values (can be NULL)
- : Optional function to handle custom string processing during option parsing
- : Byte offset in the result structure where the string value offset will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [init_string_reloption](../i/init_string_reloption.md)
  - [add_local_reloption](add_local_reloption.md)
  - RELOPT_KIND_LOCAL
  - [relopt_string](../r/relopt_string.md)
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - GET_STRING_RELOPTION (macro)

## Notes and Other Information
- This is used for defining access method specific reloptions or extension-specific parameters
- The offset parameter refers to an int-typed field that stores the offset of the actual string value in the bytea structure
- Local reloptions allow different access methods to have their own namespace of configuration options
- The function is defined in src/backend/access/common/reloptions.c:1118-1155

## Simplified Source

```c
void add_local_string_reloption(local_relopts *relopts, const char *name,
                                const char *desc, const char *default_val,
                                validate_string_relopt validator,
                                fill_string_relopt filler, int offset) {
    // Create a local string reloption with validation and filling callbacks
    relopt_string *new_option = init_string_reloption(RELOPT_KIND_LOCAL,
                                                      name, desc, default_val,
                                                      validator, filler, 0);

    // Add to the local reloptions structure with memory offset
    add_local_reloption(relopts, (relopt_gen *) new_option, offset);
}
```