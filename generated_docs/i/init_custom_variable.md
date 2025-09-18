# init_custom_variable

## Location
src/backend/utils/misc/guc.c: 4879 - 4938

## Overview
Common initialization function for custom GUC variables that allocates and configures the generic fields of a config structure for all custom variable types.

## Definition


## Detailed Description
This internal function serves as the common initialization routine for all DefineCustomXXXVariable functions. It allocates memory for a new custom GUC variable's configuration structure and fills in the generic fields that are common to all variable types. The function performs several validation checks to ensure custom variables are created safely and securely, including restrictions on PGC_POSTMASTER variables (must be created during shared library preload) and security restrictions on certain pljava variables.

## Parameters / Member Variables
- : The name of the custom GUC variable
- : Brief description of the variable
- : Detailed description of the variable (can be NULL)
- : GUC context level (determines who can set the variable)
- : Bitfield of GUC flags controlling variable behavior
- : The configuration type enum (bool, int, real, string, enum)
- : Size of the specific config structure to allocate

## Dependencies
- Functions called/Symbols referenced:
  - guc_malloc
  - guc_strdup
  - CUSTOM_OPTIONS
  - PGC_POSTMASTER, PGC_USERSET, PGC_SUSET (GucContext values)
  - GUC_LIST_QUOTE (flag constant)
- Called from (representative examples):
  - DefineCustomBoolVariable
  - DefineCustomIntVariable
  - DefineCustomRealVariable
  - DefineCustomStringVariable
  - DefineCustomEnumVariable

## Notes and Other Information
- This is a static function internal to guc.c and not exposed publicly
- Includes security hardening for pljava variables by upgrading their context from PGC_USERSET to PGC_SUSET
- Enforces that PGC_POSTMASTER variables can only be created during shared library preload
- Prohibits custom variables from using the GUC_LIST_QUOTE flag
- All custom variables are assigned to the CUSTOM_OPTIONS group for organization