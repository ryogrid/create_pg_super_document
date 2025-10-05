# find_struct_member

## Location
[src/interfaces/ecpg/preproc/variable.c:25-125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/variable.c#L25-L125)

## Overview
Recursively traverses a struct member hierarchy to find and return a specific member variable based on a dot-notation path, handling nested structs, unions, and arrays.

## Definition

```c
static struct variable *
find_struct_member(char *name, char *str, struct ECPGstruct_member *members, int brace_level)
```
## Detailed Description
This function is part of the ECPG (Embedded SQL in C) preprocessor's variable resolution system. It parses a string containing a member access path (using dot notation like "member.submember[index]") and traverses the corresponding struct member hierarchy to locate the target member. The function handles complex nested structures including:

- Simple struct/union member access with dot notation
- Array element access with bracket notation
- Nested combinations of structs, unions, and arrays
- Recursive traversal for deeply nested structures

When a member is found, it creates and returns a new variable object with the appropriate type information. The function performs syntax validation and reports errors for malformed variable references.

## Parameters / Member Variables
- `*name`: The original variable name being resolved (used for error reporting)
- `*str`: The member access string to parse (e.g., ".member[0].submember")
- `*members`: Linked list of struct members to search through
- `brace_level`: Current nesting level for brace counting
## Dependencies
- Functions called/Symbols referenced:
  - strpbrk (C library function for string parsing)
  - strcmp (C library function for string comparison)
  - [new_variable](../n/new_variable.md) (creates new variable objects)
  - [ECPGmake_array_type](../E/ECPGmake_array_type.md) (creates array type descriptors)
  - [ECPGmake_simple_type](../E/ECPGmake_simple_type.md) (creates simple type descriptors)
  - [ECPGmake_struct_type](../E/ECPGmake_struct_type.md) (creates struct/union type descriptors)
  - mmfatal (error reporting function)
  - [ECPGstruct_member](../E/ECPGstruct_member.md) (struct member type)
  - ECPGt_array, ECPGt_struct, ECPGt_union (ECPG type constants)
  - PARSE_ERROR (error constant)

- Called from (representative examples):
  - [find_struct](find_struct.md) (parent function for struct variable resolution)

## Notes and Other Information
- This is a static function, only accessible within the variable.c file
- Uses recursive calls to handle nested member access
- Performs careful string parsing with temporary null termination
- Handles bracket counting to properly skip array index expressions
- Returns NULL if the specified member path cannot be resolved
- Critical for ECPG's ability to handle complex C struct variables in embedded SQL

## Simplified Source

```c
static struct variable *find_struct_member(char *name, char *str, struct ECPGstruct_member *members, int brace_level) {
    // Parse the next component of the member access path
    char *next = strpbrk(++str, ".-[");
    char *end, c = '\0';

    if (next != NULL) {
        c = *next;
        *next = '\0';  // Temporarily terminate string
    }

    // Search for the member name
    for (; members; members = members->next) {
        if (strcmp(members->name, str) == 0) {
            if (next == NULL) {
                // End of path - create variable based on member type
                switch (members->type->type) {
                    case ECPGt_array:
                        return new_variable(name, ECPGmake_array_type(...), brace_level);
                    case ECPGt_struct:
                    case ECPGt_union:
                        return new_variable(name, ECPGmake_struct_type(...), brace_level);
                    default:
                        return new_variable(name, ECPGmake_simple_type(...), brace_level);
                }
            } else {
                // More path components - handle array access and recursion
                *next = c;  // Restore character

                if (c == '[') {
                    // Skip array bracket content
                    end = skip_array_brackets(next);
                } else {
                    end = next;
                }

                // Continue recursion based on next character
                switch (*end) {
                    case '\0':  // Array element at end
                        return handle_array_element_access(name, members, brace_level);
                    case '-':
                    case '.':
                        return find_struct_member(name, end, get_next_members(members, *end), brace_level);
                    default:
                        mmfatal(PARSE_ERROR, "incorrectly formed variable \"%s\"", name);
                }
            }
        }
    }

    return NULL;  // Member not found
}
```