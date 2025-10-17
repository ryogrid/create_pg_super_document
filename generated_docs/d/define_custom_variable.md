# define_custom_variable

## Location
[src/backend/utils/misc/guc.c:4939-5047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4939-L5047)

## Overview
Inserts a newly created custom GUC variable into the global GUC hash table, handling placeholder replacement and value migration from any existing placeholder.

## Definition
```c
static void
define_custom_variable(struct config_generic *variable)
```

## Detailed Description
This function completes the custom variable definition process by integrating the variable into PostgreSQL's GUC system. It first checks if a placeholder exists for the variable name in the hash table. If no placeholder exists, it simply initializes the variable and adds it to the system. If a placeholder exists (created when the variable was referenced before being defined), it replaces the placeholder with the real variable definition and carefully migrates any stored values from the placeholder to the new variable. The function handles both current values and stacked values, applying them in the correct order while providing appropriate error handling.

## Parameters / Member Variables
- `variable`: Pointer to the fully initialized config_generic structure representing the custom variable to be defined

## Dependencies
- Functions called/Symbols referenced:
  - [check_GUC_init](../c/check_GUC_init.md)
  - [hash_search](../h/hash_search.md)
  - [InitializeOneGUCOption](../I/InitializeOneGUCOption.md)
  - [add_guc_variable](../a/add_guc_variable.md)
  - [RemoveGUCFromLists](../R/RemoveGUCFromLists.md)
  - [set_config_option_ext](../s/set_config_option_ext.md)
  - [reapply_stacked_values](../r/reapply_stacked_values.md)
  - [set_config_sourcefile](../s/set_config_sourcefile.md)
  - [set_string_field](../s/set_string_field.md)
  - [guc_free](../g/guc_free.md)
  - GUC_CUSTOM_PLACEHOLDER (flag)
  - HASH_FIND, GUC_ACTION_SET (constants)
- Called from (representative examples):
  - [DefineCustomBoolVariable](../D/DefineCustomBoolVariable.md)
  - [DefineCustomIntVariable](../D/DefineCustomIntVariable.md)
  - [DefineCustomRealVariable](../D/DefineCustomRealVariable.md)
  - [DefineCustomStringVariable](../D/DefineCustomStringVariable.md)
  - [DefineCustomEnumVariable](../D/DefineCustomEnumVariable.md)

## Notes and Other Information
- This is a static function internal to guc.c and not exposed publicly
- Handles the complex case of replacing placeholders that were created when a variable was SET before being defined
- Uses WARNING level errors for invalid values during placeholder replacement to avoid breaking module loading
- Carefully preserves source location information from placeholders
- Memory management includes cleanup of placeholder structures while being conservative about stack items to avoid complex deallocation
- Validates that variable initialization is consistent between initial and default values via check_GUC_init

## Simplified Source

```c
static void define_custom_variable(struct config_generic *variable) {
    const char *name = variable->name;
    GUCHashEntry *hentry;
    struct config_string *pHolder;

    // Validate variable initialization
    Assert(check_GUC_init(variable));

    // Look for existing placeholder
    hentry = (GUCHashEntry *) hash_search(guc_hashtab, &name, HASH_FIND, NULL);

    if (hentry == NULL) {
        // No placeholder - simple case: initialize and add
        InitializeOneGUCOption(variable);
        add_guc_variable(variable, ERROR);
        return;
    }

    // Verify this is actually a placeholder
    if ((hentry->gucvar->flags & GUC_CUSTOM_PLACEHOLDER) == 0)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                       errmsg("attempt to redefine parameter \"%s\"", name)));

    Assert(hentry->gucvar->vartype == PGC_STRING);
    pHolder = (struct config_string *) hentry->gucvar;

    // Initialize the new variable to its default
    InitializeOneGUCOption(variable);

    // Replace placeholder in hash table
    hentry->gucname = name;
    hentry->gucvar = variable;

    // Remove placeholder from lists
    RemoveGUCFromLists(&pHolder->gen);

    // Apply placeholder's reset value if any
    if (pHolder->reset_val)
        (void) set_config_option_ext(name, pHolder->reset_val,
                                    pHolder->gen.reset_scontext,
                                    pHolder->gen.reset_source,
                                    pHolder->gen.reset_srole,
                                    GUC_ACTION_SET, true, WARNING, false);

    Assert(variable->stack == NULL);

    // Apply current and stacked values from placeholder
    reapply_stacked_values(variable, pHolder, pHolder->gen.stack,
                          *(pHolder->variable),
                          pHolder->gen.scontext, pHolder->gen.source,
                          pHolder->gen.srole);

    // Copy source location information
    if (pHolder->gen.sourcefile)
        set_config_sourcefile(name, pHolder->gen.sourcefile,
                             pHolder->gen.sourceline);

    // Clean up placeholder memory
    set_string_field(pHolder, pHolder->variable, NULL);
    set_string_field(pHolder, &pHolder->reset_val, NULL);
    guc_free(pHolder);
}
```