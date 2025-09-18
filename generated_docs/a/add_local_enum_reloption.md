# add_local_enum_reloption

## Location
src/backend/access/common/reloptions.c: 1036 - 1052

## Overview
The add_local_enum_reloption function adds a new local enumeration-type reloption (relation option) with specified enum values and target field offset.

## Definition
void add_local_enum_reloption(local_relopts *relopts, const char *name, const char *desc, relopt_enum_elt_def *members, int default_val, const char *detailmsg, int offset)

## Detailed Description
This function creates and registers a new local enumeration-type relation option within the PostgreSQL reloptions system. It serves as a wrapper that first initializes an enum reloption using init_enum_reloption() with the RELOPT_KIND_LOCAL kind, then adds it to the local reloptions structure using add_local_reloption(). The function is designed for access methods and extensions that need enum-based configuration options that are specific to individual relation instances rather than being globally available. The enum values are stored as integers in the target structure at the specified offset.

## Parameters / Member Variables
- : Pointer to the local_relopts structure where the new option will be added
- : String identifier for the reloption name
- : Human-readable description of the option's purpose  
- : Pointer to array of relopt_enum_elt_def structures defining valid enum values
- : Integer index of the default enum value within the members array
- : Error message shown for invalid values, providing list of valid options
- : Byte offset of the int-typed field in the target structure where enum value will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [init_enum_reloption](../i/init_enum_reloption.md)
  - [add_local_reloption](add_local_reloption.md)
  - RELOPT_KIND_LOCAL
  - [relopt_enum_elt_def](../r/relopt_enum_elt_def.md)
  - [relopt_enum](../r/relopt_enum.md)
  - [relopt_gen](../r/relopt_gen.md)
- Called from (representative examples):
  - Used in reloptions header macros

## Notes and Other Information
- This function is specifically for local (non-global) relation options
- The offset parameter must correspond to an int-typed field in the target structure
- Unlike the global version, this uses RELOPT_KIND_LOCAL and lockmode 0
- The members array and detailmsg must remain valid for the option's lifetime
- Part of PostgreSQL's extensible relation options framework for access method-specific configuration
- Enum values are stored as integer indices rather than string values