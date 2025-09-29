# fillRelOptions

## Location
[src/backend/access/common/reloptions.c:1751-1846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1751-L1846)

## Overview
A static function that fills a previously allocated relation options structure with parsed option values, handling different data types and string storage.

## Definition

```c
enum_val :
							((relopt_enum *) options[i].gen)->default_val;
```
## Detailed Description
This function takes the output from parseRelOptions and fills a relation options structure that was previously allocated with allocateReloptStruct. It iterates through all provided options and matches them against a parsing table, then stores the values in the appropriate locations within the structure. The function handles different data types (bool, int, real, enum, string) and manages string storage by copying strings to the end of the structure and storing offsets. For string options, it supports both default values and custom fill callbacks.

## Parameters / Member Variables
- : Pointer to the allocated relation options structure to be filled
- : Size of the base structure that was passed to allocateReloptStruct
- : Array of parsed option values from parseRelOptions
- : Number of elements in the options array
- : When true, expects all options to appear in the parsing table
- : Parsing table describing allowed options and their properties
- : Number of elements in the parsing table

## Dependencies
- Functions called/Symbols referenced:
  - [relopt_value](../r/relopt_value.md) (struct type)
  - relopt_parse_elt (struct type) 
  - [relopt_string](../r/relopt_string.md), relopt_bool, relopt_int, relopt_real, relopt_enum (struct types)
  - RELOPT_TYPE_BOOL, RELOPT_TYPE_INT, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM, RELOPT_TYPE_STRING (enum values)
  - SET_VARSIZE (macro)
  - strcmp, strcpy, strlen (standard C functions)
  - elog (PostgreSQL logging function)
- Called from:
  - [build_reloptions](../b/build_reloptions.md) (src/backend/access/common/reloptions.c:1940)
  - [build_local_reloptions](../b/build_local_reloptions.md) (src/backend/access/common/reloptions.c:1976)

## Notes and Other Information
- This is a static function, only accessible within reloptions.c
- Handles variable-length string storage by placing strings at the end of the structure and storing offsets
- Supports custom fill callbacks for string options that need special processing
- Uses SET_VARSIZE to set the final size of the variable-length structure
- Error handling includes validation of option names when validate=true
- [String](../S/String.md) handling includes support for NULL values and default values

## Simplified Source

```c
static void fillRelOptions(void *rdopts, Size basesize,
                          relopt_value *options, int numoptions,
                          bool validate,
                          const relopt_parse_elt *elems, int numelems) {
    int offset = basesize;

    // Process each parsed option
    for (int i = 0; i < numoptions; i++) {
        bool found = false;

        // Find matching element in parsing table
        for (int j = 0; j < numelems; j++) {
            if (strcmp(options[i].gen->name, elems[j].optname) == 0) {
                char *target_location = ((char *) rdopts) + elems[j].offset;

                // Handle different data types
                switch (options[i].gen->type) {
                    case RELOPT_TYPE_BOOL:
                        *(bool *) target_location = options[i].isset ?
                            options[i].values.bool_val :
                            ((relopt_bool *) options[i].gen)->default_val;
                        break;

                    case RELOPT_TYPE_INT:
                        *(int *) target_location = options[i].isset ?
                            options[i].values.int_val :
                            ((relopt_int *) options[i].gen)->default_val;
                        break;

                    case RELOPT_TYPE_REAL:
                        *(double *) target_location = options[i].isset ?
                            options[i].values.real_val :
                            ((relopt_real *) options[i].gen)->default_val;
                        break;

                    case RELOPT_TYPE_ENUM:
                        *(int *) target_location = options[i].isset ?
                            options[i].values.enum_val :
                            ((relopt_enum *) options[i].gen)->default_val;
                        break;

                    case RELOPT_TYPE_STRING:
                        // Handle string storage with offsets
                        relopt_string *optstring = (relopt_string *) options[i].gen;
                        char *string_val = get_string_value(&options[i], optstring);

                        if (optstring->fill_cb) {
                            // Custom callback for complex strings
                            Size size = optstring->fill_cb(string_val, (char *) rdopts + offset);
                            *(int *) target_location = size ? offset : 0;
                            if (size) offset += size;
                        } else if (string_val) {
                            // Standard string: copy and store offset
                            strcpy((char *) rdopts + offset, string_val);
                            *(int *) target_location = offset;
                            offset += strlen(string_val) + 1;
                        } else {
                            *(int *) target_location = 0;  // NULL string
                        }
                        break;
                }
                found = true;
                break;
            }
        }

        // Validate that all options were found if required
        if (validate && !found) {
            elog(ERROR, "reloption \"%s\" not found in parse table", options[i].gen->name);
        }
    }

    // Set final size of variable-length structure
    SET_VARSIZE(rdopts, offset);
}
```