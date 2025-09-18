# init_enum_reloption

## Location
[src/backend/access/common/reloptions.c:989-1017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L989-L1017)

## Overview
The init_enum_reloption function allocates and initializes a new enumeration-type reloption (relation option) with specified enum members and default value.

## Definition
static relopt_enum *init_enum_reloption(bits32 kinds, const char *name, const char *desc, relopt_enum_elt_def *members, int default_val, const char *detailmsg, LOCKMODE lockmode)

## Detailed Description
This static function serves as a constructor for enumeration-type relation options within PostgreSQL's reloptions framework. It allocates memory for a new relopt_enum structure using the generic allocate_reloption() function with RELOPT_TYPE_ENUM type, then initializes the enum-specific fields including the list of valid enum members, default value, and detail message. The function is used internally by higher-level functions that need to create enum-based configuration options for tables, indexes, or other database objects.

## Parameters / Member Variables
- : Bitmask specifying which relation kinds (tables, indexes, etc.) this option applies to
- : String identifier for the reloption name
- : Human-readable description of the option's purpose
- : Pointer to array of relopt_enum_elt_def structures defining valid enum values
- : Integer index of the default enum value within the members array
- : Additional detail message for error reporting or help text
- : Required lock mode when this option is modified

## Dependencies
- Functions called/Symbols referenced:
  - [allocate_reloption](../a/allocate_reloption.md)
  - RELOPT_TYPE_ENUM
  - [relopt_enum_elt_def](../r/relopt_enum_elt_def.md)
  - bits32
- Called from (representative examples):
  - [add_enum_reloption](../a/add_enum_reloption.md)
  - [add_local_enum_reloption](../a/add_local_enum_reloption.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reloptions.c file
- The function performs the low-level initialization work for enum reloptions
- The members parameter must remain valid for the lifetime of the reloption
- The default_val must be a valid index into the members array
- Part of PostgreSQL's type-safe approach to configuration parameters