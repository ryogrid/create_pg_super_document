# relopt_enum

## Location
[src/include/access/reloptions.h:123-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/reloptions.h#L123-L130)

## Overview
A structure that defines an enumerated relation option in PostgreSQL's relation options system, containing metadata and valid values for enum-type storage parameters.

## Definition
```c
typedef struct relopt_enum
{
    relopt_gen  gen;
    relopt_enum_elt_def *members;
    int         default_val;
    const char *detailmsg;
    /* null-terminated array of members */
} relopt_enum;
```

## Detailed Description
The `relopt_enum` structure represents a complete enumerated relation option definition. It extends the base `relopt_gen` structure and adds enum-specific functionality by providing an array of valid enum values (members), a default value, and detailed error messaging. This structure enables PostgreSQL to handle relation options that accept a limited set of predefined string values, such as compression methods or storage formats.

## Parameters / Member Variables
- `gen`: Base relation option metadata inherited from relopt_gen structure
- `members`: Null-terminated array of relopt_enum_elt_def structures defining valid enum values
- `default_val`: The default integer value to use when the option is not explicitly specified
- `detailmsg`: Detailed error message to display when an invalid enum value is provided

## Dependencies
- Functions called/Symbols referenced:
  - [relopt_gen](relopt_gen.md) (Line 125)
  - [relopt_enum_elt_def](relopt_enum_elt_def.md) (Line 126)
- Called from (representative examples):
  - [allocate_reloption](../a/allocate_reloption.md) (src/backend/access/common/reloptions.c:799)
  - [init_enum_reloption](../i/init_enum_reloption.md) (src/backend/access/common/reloptions.c:993, 995)
  - [add_enum_reloption](../a/add_enum_reloption.md) (src/backend/access/common/reloptions.c:1022)
  - [add_local_enum_reloption](../a/add_local_enum_reloption.md) (src/backend/access/common/reloptions.c:1040)
  - [parse_one_reloption](../p/parse_one_reloption.md) (src/backend/access/common/reloptions.c:1651)
  - [fillRelOptions](../f/fillRelOptions.md) (src/backend/access/common/reloptions.c:1792)

## Notes and Other Information
This structure is part of PostgreSQL's type-safe relation options system. The members array must be null-terminated to indicate the end of valid enum values. The detailmsg field provides user-friendly error messages when validation fails, helping users understand what values are acceptable for the enum option.