# init_custom_variable

## Location
[src/backend/utils/misc/guc.c:4879-4938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4879-L4938)

## Overview
Common initialization function for custom GUC variables that allocates and configures the generic fields of a config structure for all custom variable types.

## Definition

```c
static struct config_generic *
init_custom_variable(const char *name,
					 const char *short_desc,
					 const char *long_desc,
					 GucContext context,
					 int flags,
					 enum config_type type,
					 size_t sz)
```
## Detailed Description
This internal function serves as the common initialization routine for all DefineCustomXXXVariable functions. It allocates memory for a new custom GUC variable's configuration structure and fills in the generic fields that are common to all variable types. The function performs several validation checks to ensure custom variables are created safely and securely, including restrictions on PGC_POSTMASTER variables (must be created during shared library preload) and security restrictions on certain pljava variables.

## Parameters / Member Variables
- `*name`: The name of the custom GUC variable
- `*short_desc`: Brief description of the variable
- `*long_desc`: Detailed description of the variable (can be NULL)
- `context`: GUC context level (determines who can set the variable)
- `flags`: Bitfield of GUC flags controlling variable behavior
- `type`: The configuration type enum (bool, int, real, string, enum)
- `sz`: Size of the specific config structure to allocate
## Dependencies
- Functions called/Symbols referenced:
  - [guc_malloc](../g/guc_malloc.md)
  - [guc_strdup](../g/guc_strdup.md)
  - CUSTOM_OPTIONS
  - PGC_POSTMASTER, PGC_USERSET, PGC_SUSET (GucContext values)
  - GUC_LIST_QUOTE (flag constant)
- Called from (representative examples):
  - [DefineCustomBoolVariable](../D/DefineCustomBoolVariable.md)
  - [DefineCustomIntVariable](../D/DefineCustomIntVariable.md)
  - [DefineCustomRealVariable](../D/DefineCustomRealVariable.md)
  - [DefineCustomStringVariable](../D/DefineCustomStringVariable.md)
  - [DefineCustomEnumVariable](../D/DefineCustomEnumVariable.md)

## Notes and Other Information
- This is a static function internal to guc.c and not exposed publicly
- Includes security hardening for pljava variables by upgrading their context from PGC_USERSET to PGC_SUSET
- Enforces that PGC_POSTMASTER variables can only be created during shared library preload
- Prohibits custom variables from using the GUC_LIST_QUOTE flag
- All custom variables are assigned to the CUSTOM_OPTIONS group for organization