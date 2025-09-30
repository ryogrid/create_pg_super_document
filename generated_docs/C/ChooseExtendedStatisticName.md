# ChooseExtendedStatisticName

## Location
[src/backend/commands/statscmds.c:809-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L809-L850)

## Overview
Selects a nonconflicting name for a new PostgreSQL extended statistics object by appending digits to a label if necessary to ensure uniqueness within the specified namespace.

## Definition

```c
static char *
ChooseExtendedStatisticName(const char *name1, const char *name2,
							const char *label, Oid namespaceid)
```
## Detailed Description
This function generates a unique name for an extended statistics object by combining name components and ensuring no conflicts exist in the target namespace. It starts with the unmodified label and incrementally appends digits (e.g., "_1", "_2") until finding a name that doesn't already exist in the pg_statistic_ext system catalog. The function uses the same naming convention as makeObjectName() but with additional conflict resolution logic specific to extended statistics objects.

The function performs a loop that:
1. Constructs a candidate name using makeObjectName()
2. Checks if the name already exists using GetSysCacheOid2()
3. If a conflict is found, increments a counter and tries again with a modified label
4. Returns the first non-conflicting name found

## Parameters / Member Variables
- : First component of the object name (typically relation name)
- : Second component of the object name (can be NULL)
- : Base label for the statistics object (cannot be NULL)
- : OID of the namespace where the statistics object will be created

## Dependencies
- Functions called/Symbols referenced:
  - [strlcpy](../s/strlcpy.md)
  - [makeObjectName](../m/makeObjectName.md)
  - GetSysCacheOid2
  - NAMEDATALEN (constant)
- Called from (representative examples):
  - [CreateStatistics](CreateStatistics.md)

## Notes and Other Information
- The function includes a theoretical race condition warning: concurrent sessions could choose the same name, though this is unlikely in practice when holding appropriate locks
- When creating multiple statistics objects in a single command, the caller should create each object and call CommandCounterIncrement before choosing the next name
- Returns a palloc'd string that must be freed by the caller
- Located in src/backend/commands/statscmds.c (lines 809-850)

## Simplified Source

```c
static char *
ChooseExtendedStatisticName(const char *name1, const char *name2,
                           const char *label, Oid namespaceid)
{
    int pass = 0;
    char *stxname = NULL;
    char modlabel[NAMEDATALEN];

    // Start with the unmodified label
    strlcpy(modlabel, label, sizeof(modlabel));

    // Keep trying until we find a unique name
    for (;;)
    {
        Oid existingstats;

        // Construct candidate name
        stxname = makeObjectName(name1, name2, modlabel);

        // Check if name already exists in the namespace
        existingstats = GetSysCacheOid2(STATEXTNAMENSP, Anum_pg_statistic_ext_oid,
                                       PointerGetDatum(stxname),
                                       ObjectIdGetDatum(namespaceid));

        // If name is unique, we're done
        if (!OidIsValid(existingstats))
            break;

        // Name conflict - try again with a number suffix
        pfree(stxname);
        snprintf(modlabel, sizeof(modlabel), "%s%d", label, ++pass);
    }

    return stxname;
}
```