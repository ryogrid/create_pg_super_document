# MarkGUCPrefixReserved

## Location
[src/backend/utils/misc/guc.c:5287-5338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L5287-L5338)

## Overview
Marks a GUC prefix as reserved, preventing creation of new placeholder variables with that prefix and removing any existing placeholders, helping extensions validate their configuration namespace.

## Definition
```c
void MarkGUCPrefixReserved(const char *className)
```

## Detailed Description
This function is part of PostgreSQL's configuration system safety mechanism. It allows extensions to declare that they "own" a particular GUC prefix (e.g., "myext.") and prevents accidentally created placeholder variables from occupying their namespace. When PostgreSQL encounters an unknown configuration variable in postgresql.conf, it creates a placeholder entry. This function removes any existing placeholders that match the reserved prefix and adds the prefix to a list to prevent future placeholder creation.

Extensions typically call this function after defining all their custom GUC variables to ensure their namespace is protected from typos in configuration files. The function scans the entire GUC hash table to find and remove conflicting placeholders, issuing warnings about any removed entries.

## Parameters / Member Variables
- `className`: The GUC prefix to reserve (e.g., "myextension" for variables like "myextension.setting")

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_search](../h/hash_search.md)
  - [RemoveGUCFromLists](../R/RemoveGUCFromLists.md)
  - [GUCHashEntry](../G/GUCHashEntry.md)
  - [config_generic](../c/config_generic.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - GUC_CUSTOM_PLACEHOLDER
  - GUC_QUALIFIER_SEPARATOR
  - HASH_REMOVE
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (plperl.c:456)
  - [_PG_init](../P/_PG_init.md) (pltcl.c:480, pltcl.c:481)
  - [_PG_init](../P/_PG_init.md) (delay_execution.c:90)
  - [_PG_init](../P/_PG_init.md) (ssl_passphrase_func.c:49)
  - [_PG_init](../P/_PG_init.md) (test_oat_hooks.c:211)
  - [_PG_init](../P/_PG_init.md) (worker_spi.c:363)

## Notes and Other Information
This function should be called after an extension has finished defining all its custom GUC variables, typically at the end of the _PG_init function. The reserved prefix string is duplicated and stored in GUCMemoryContext, so it persists for the lifetime of the process. The function generates warnings when removing existing placeholders, alerting administrators to potential configuration errors. The prefix matching includes the GUC_QUALIFIER_SEPARATOR (typically '.'), so reserving "myext" prevents placeholders like "myext.anything" but not "myextother.setting".

## Simplified Source

```c
void MarkGUCPrefixReserved(const char *className)
{
    int classLen = strlen(className);
    HASH_SEQ_STATUS status;
    GUCHashEntry *hentry;
    MemoryContext oldcontext;

    // Search for existing placeholder variables with this prefix
    hash_seq_init(&status, guc_hashtab);
    while ((hentry = (GUCHashEntry *) hash_seq_search(&status)) != NULL)
    {
        struct config_generic *var = hentry->gucvar;

        // Check if this is a placeholder with our prefix
        if ((var->flags & GUC_CUSTOM_PLACEHOLDER) != 0 &&
            strncmp(className, var->name, classLen) == 0 &&
            var->name[classLen] == GUC_QUALIFIER_SEPARATOR)
        {
            // Warn about removing invalid placeholder
            ereport(WARNING,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("invalid configuration parameter name \"%s\", removing it",
                            var->name),
                     errdetail("\"%s\" is now a reserved prefix.",
                               className)));

            // Remove from hash table and lists
            hash_search(guc_hashtab, &var->name, HASH_REMOVE, NULL);
            RemoveGUCFromLists(var);
        }
    }

    // Add prefix to reserved list to prevent future placeholders
    oldcontext = MemoryContextSwitchTo(GUCMemoryContext);
    reserved_class_prefix = lappend(reserved_class_prefix, pstrdup(className));
    MemoryContextSwitchTo(oldcontext);
}
```