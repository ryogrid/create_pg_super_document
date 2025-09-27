# build_guc_variables

## Location
[src/backend/utils/misc/guc.c:905-1048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L905-L1048)

## Overview
Builds the GUC (Grand Unified Configuration) hash table by counting all built-in configuration variables and populating the global hash table for efficient variable lookup.

## Definition

```c
struct config_bool *conf = &ConfigureNamesBool[i];
```
## Detailed Description
This function is responsible for initializing PostgreSQL's configuration variable system by creating and populating the main GUC hash table. It performs the following key operations:

1. **Memory Context Creation**: Creates a dedicated memory context (GUCMemoryContext) for all GUC-related data structures
2. **Variable Type Assignment**: Iterates through all built-in configuration variable arrays and assigns appropriate type identifiers (PGC_BOOL, PGC_INT, PGC_REAL, PGC_STRING, PGC_ENUM)
3. **Variable Counting**: Counts the total number of built-in variables across all types
4. **Hash Table Creation**: Creates a hash table with 20% slack space for efficient lookup operations
5. **Variable Registration**: Adds all built-in variables to the hash table for fast name-based lookup

The function is split out from InitializeGUCOptions to allow help_config.c to extract variable information without running the full initialization process. It processes five different types of configuration variables: boolean, integer, real (floating-point), string, and enumerated values.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [hash_create](../h/hash_create.md)
  - [hash_search](../h/hash_search.md)
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [guc_name_hash](../g/guc_name_hash.md)
  - [guc_name_match](../g/guc_name_match.md)
- Data structures used:
  - ConfigureNamesBool
  - ConfigureNamesInt
  - ConfigureNamesReal
  - ConfigureNamesString
  - ConfigureNamesEnum
  - [HASHCTL](../H/HASHCTL.md)
  - [GUCHashEntry](../G/GUCHashEntry.md)
- Called from:
  - [InitializeGUCOptions](../I/InitializeGUCOptions.md) (src/backend/utils/misc/guc.c:1546)
  - [GucInfoMain](../G/GucInfoMain.md) (src/backend/utils/misc/help_config.c:53)

## Notes and Other Information
- The function creates the hash table with 25% extra capacity (num_vars + num_vars/4) to minimize collisions
- All built-in variables are processed in a specific order: bool, int, real, string, enum
- The function uses assertions to ensure no duplicate variable names exist during registration
- Memory allocation is performed in the GUCMemoryContext to facilitate cleanup and memory management
- This function is not intended for general use outside of GUC system initialization

## Simplified Source

```c
// Simplified version of build_guc_variables
void build_guc_variables(void) {
    int num_vars = 0;
    HASHCTL hash_ctl;
    GUCHashEntry *hentry;
    bool found;
    int i;

    // Step 1: Create dedicated memory context for GUC data
    GUCMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                           "GUCMemoryContext",
                                           ALLOCSET_DEFAULT_SIZES);

    // Step 2: Count all built-in variables and set their types
    // Process boolean variables
    for (i = 0; ConfigureNamesBool[i].gen.name; i++) {
        ConfigureNamesBool[i].gen.vartype = PGC_BOOL;
        num_vars++;
    }

    // Process integer variables
    for (i = 0; ConfigureNamesInt[i].gen.name; i++) {
        ConfigureNamesInt[i].gen.vartype = PGC_INT;
        num_vars++;
    }

    // Process real variables
    for (i = 0; ConfigureNamesReal[i].gen.name; i++) {
        ConfigureNamesReal[i].gen.vartype = PGC_REAL;
        num_vars++;
    }

    // Process string variables
    for (i = 0; ConfigureNamesString[i].gen.name; i++) {
        ConfigureNamesString[i].gen.vartype = PGC_STRING;
        num_vars++;
    }

    // Process enum variables
    for (i = 0; ConfigureNamesEnum[i].gen.name; i++) {
        ConfigureNamesEnum[i].gen.vartype = PGC_ENUM;
        num_vars++;
    }

    // Step 3: Create hash table with 25% extra capacity
    int size_vars = num_vars + num_vars / 4;

    hash_ctl.keysize = sizeof(char *);
    hash_ctl.entrysize = sizeof(GUCHashEntry);
    hash_ctl.hash = guc_name_hash;
    hash_ctl.match = guc_name_match;
    hash_ctl.hcxt = GUCMemoryContext;

    guc_hashtab = hash_create("GUC hash table", size_vars, &hash_ctl,
                             HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

    // Step 4: Add all variables to the hash table
    // Add boolean variables
    for (i = 0; ConfigureNamesBool[i].gen.name; i++) {
        struct config_generic *gucvar = &ConfigureNamesBool[i].gen;
        hentry = (GUCHashEntry *) hash_search(guc_hashtab, &gucvar->name, HASH_ENTER, &found);
        hentry->gucvar = gucvar;
    }

    // Add integer variables
    for (i = 0; ConfigureNamesInt[i].gen.name; i++) {
        struct config_generic *gucvar = &ConfigureNamesInt[i].gen;
        hentry = (GUCHashEntry *) hash_search(guc_hashtab, &gucvar->name, HASH_ENTER, &found);
        hentry->gucvar = gucvar;
    }

    // Add real variables
    for (i = 0; ConfigureNamesReal[i].gen.name; i++) {
        struct config_generic *gucvar = &ConfigureNamesReal[i].gen;
        hentry = (GUCHashEntry *) hash_search(guc_hashtab, &gucvar->name, HASH_ENTER, &found);
        hentry->gucvar = gucvar;
    }

    // Add string variables
    for (i = 0; ConfigureNamesString[i].gen.name; i++) {
        struct config_generic *gucvar = &ConfigureNamesString[i].gen;
        hentry = (GUCHashEntry *) hash_search(guc_hashtab, &gucvar->name, HASH_ENTER, &found);
        hentry->gucvar = gucvar;
    }

    // Add enum variables
    for (i = 0; ConfigureNamesEnum[i].gen.name; i++) {
        struct config_generic *gucvar = &ConfigureNamesEnum[i].gen;
        hentry = (GUCHashEntry *) hash_search(guc_hashtab, &gucvar->name, HASH_ENTER, &found);
        hentry->gucvar = gucvar;
    }
}
```

Key simplifications made:
- Removed Assert() statements for clarity
- Consolidated repetitive hash table insertion logic
- Added descriptive comments for each major step
- Focused on the main execution flow
- Abstracted hash table configuration details into clear steps
- Maintained the essential algorithm while improving readability