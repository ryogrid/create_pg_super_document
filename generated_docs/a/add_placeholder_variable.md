# add_placeholder_variable

## Location
[src/backend/utils/misc/guc.c:1179-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1179-L1236)

## Overview
Creates and registers a placeholder variable for a custom configuration variable name that hasn't been formally defined yet.

## Definition
```c
static struct config_generic *add_placeholder_variable(const char *name, int elevel)
```

## Detailed Description
This function creates a temporary placeholder for custom GUC variables that are referenced before being formally defined. Placeholder variables allow PostgreSQL to accept and store values for custom configuration parameters even when the extension that defines them hasn't been loaded yet.

The function performs several key operations:
1. **Memory allocation**: Allocates memory for a config_string structure plus space for a char pointer
2. **Structure initialization**: Sets up a minimal config_generic structure with appropriate defaults
3. **Name storage**: Makes a copy of the variable name using guc_strdup()
4. **Configuration setup**: Configures the placeholder with CUSTOM_OPTIONS group, PGC_USERSET context, and special flags
5. **Variable registration**: Adds the placeholder to the global GUC hash table

Placeholder variables have special characteristics: they're marked with GUC_CUSTOM_PLACEHOLDER flag, hidden from SHOW ALL commands (GUC_NO_SHOW_ALL), and excluded from sample configurations (GUC_NOT_IN_SAMPLE).

## Parameters / Member Variables
- `name`: The name of the custom variable for which to create a placeholder
- `elevel`: Error level to use when reporting memory allocation failures

## Dependencies
- Functions called/Symbols referenced:
  - [guc_malloc](../g/guc_malloc.md)
  - [guc_strdup](../g/guc_strdup.md)
  - [guc_free](../g/guc_free.md)
  - [add_guc_variable](add_guc_variable.md)
  - memset
  - unconstify
- Data structures used:
  - [config_string](../c/config_string.md)
  - [config_generic](../c/config_generic.md)
- Constants used:
  - PGC_USERSET
  - CUSTOM_OPTIONS
  - GUC_NO_SHOW_ALL
  - GUC_NOT_IN_SAMPLE
  - GUC_CUSTOM_PLACEHOLDER
  - PGC_STRING
- Called from:
  - [find_option](../f/find_option.md) (src/backend/utils/misc/guc.c:1271)

## Notes and Other Information
- This is a static function, only accessible within the guc.c file
- Returns a pointer to the created config_generic structure on success, NULL on memory allocation failure
- The placeholder variable's storage pointer is allocated at the end of the structure for efficiency
- All value pointers (current, boot, reset) start as NULL
- Placeholder variables can be later replaced by properly defined custom variables when extensions are loaded
- Memory management includes proper cleanup on allocation failures
- The function uses specialized GUC memory allocation functions (guc_malloc, guc_strdup, guc_free) for consistent memory management

## Simplified Source

```c
static struct config_generic *add_placeholder_variable(const char *name, int elevel) {
    size_t sz = sizeof(struct config_string) + sizeof(char *);
    struct config_string *var;
    struct config_generic *gen;

    // Allocate memory for placeholder variable
    var = (struct config_string *) guc_malloc(elevel, sz);
    if (var == NULL)
        return NULL;

    memset(var, 0, sz);
    gen = &var->gen;

    // Store variable name
    gen->name = guc_strdup(elevel, name);
    if (gen->name == NULL) {
        guc_free(var);
        return NULL;
    }

    // Set placeholder configuration
    gen->context = PGC_USERSET;
    gen->group = CUSTOM_OPTIONS;
    gen->short_desc = "GUC placeholder variable";
    gen->flags = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER;
    gen->vartype = PGC_STRING;

    // Set variable pointer at end of struct
    var->variable = (char **) (var + 1);

    // Add to GUC hash table
    if (!add_guc_variable((struct config_generic *) var, elevel)) {
        guc_free(unconstify(char *, gen->name));
        guc_free(var);
        return NULL;
    }

    return gen;
}
```