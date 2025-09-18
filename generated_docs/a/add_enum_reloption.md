# add_enum_reloption

## Location
[src/backend/access/common/reloptions.c:1018-1035](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1018-L1035)

## Overview
The add_enum_reloption function adds a new global enumeration-type reloption (relation option) to PostgreSQL's reloptions system with specified enum values and validation.

## Definition
void add_enum_reloption(bits32 kinds, const char *name, const char *desc, relopt_enum_elt_def *members, int default_val, const char *detailmsg, LOCKMODE lockmode)

## Detailed Description
This function creates and registers a new global enumeration-type relation option within PostgreSQL's extensible reloptions framework. It serves as a high-level wrapper that first initializes an enum reloption using init_enum_reloption(), then adds it to the global reloptions registry using add_reloption(). The function is designed for access methods and extensions that need to define enum-based configuration parameters with a predefined set of valid values. The members array and detailmsg are not copied, so the caller must ensure their validity throughout the process lifetime.

## Parameters / Member Variables
- : Bitmask specifying which relation kinds (tables, indexes, etc.) this option applies to
- : String identifier for the reloption name  
- : Human-readable description of the option's purpose
- : Pointer to NULL-terminated array of relopt_enum_elt_def structures defining valid enum values
- : Integer index of the default enum value within the members array
- : Error message template shown for invalid values, format: "Valid values are \"foo\", \"bar\", and \"baz\""
- : Required lock mode when this option is modified

## Dependencies
- Functions called/Symbols referenced:
  - [init_enum_reloption](../i/init_enum_reloption.md)
  - [add_reloption](add_reloption.md)
  - [relopt_enum_elt_def](../r/relopt_enum_elt_def.md)
  - [relopt_enum](../r/relopt_enum.md)
  - [relopt_gen](../r/relopt_gen.md)
  - bits32
- Called from (representative examples):
  - [create_reloptions_table](../c/create_reloptions_table.md) (in test modules)

## Notes and Other Information
- This creates global reloptions (as opposed to local ones)
- The members array must be NULL-terminated
- Both members array and detailmsg must remain valid for the process lifetime (not copied)
- The detailmsg provides user-friendly error messages when invalid enum values are specified
- Used by access methods and extensions to define enum-based configuration options
- Part of PostgreSQL's type-safe configuration parameter system