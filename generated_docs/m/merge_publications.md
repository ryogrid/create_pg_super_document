# merge_publications

## Location
[src/backend/commands/subscriptioncmds.c:2332-2390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2332-L2390)

## Overview
Merges current subscription's publications with user-specified publications for ADD/DROP PUBLICATIONS operations, ensuring proper validation and consistency.

## Definition

```c
static List *
merge_publications(List *oldpublist, List *newpublist, bool addpub, const char *subname)
```
## Detailed Description
The  function is a static helper function used in subscription management commands to safely merge publication lists. It handles both adding publications to a subscription (ADD PUBLICATIONS) and removing publications from a subscription (DROP PUBLICATIONS). The function creates a copy of the original publication list and performs the requested merge operation while ensuring data consistency and proper error handling.

The function validates that publications being added don't already exist in the subscription, and that publications being dropped actually exist in the subscription. It prevents the dangerous operation of dropping all publications from a subscription, which would leave the subscription in an invalid state.

## Parameters
- : The current list of publications in the subscription
- : The list of publications to add or remove  
- : Boolean flag indicating the operation type (true for ADD, false for DROP)
- : The subscription name, used for error messages

## Dependencies
- Functions called/Symbols referenced:
  - [list_copy](../l/list_copy.md)
  - [check_duplicates_in_publist](../c/check_duplicates_in_publist.md)
  - ERRCODE_DUPLICATE_OBJECT
  - foreach_delete_current
  - [makeString](makeString.md)
- Called from (representative examples):
  - [AlterSubscription](../A/AlterSubscription.md)

## Notes and Other Information
- The function creates a copy of the original list, leaving the input list unchanged
- Uses  to validate the new publication list for duplicates
- Enforces the constraint that a subscription must always have at least one publication
- Error messages include the subscription name for better user experience
- The XXX comment indicates that preventing empty publication lists maintains consistency with SET PUBLICATION behavior
- Part of PostgreSQL's logical replication subscription management system

## Simplified Source

```c
static List *
merge_publications(List *oldpublist, List *newpublist, bool addpub, const char *subname)
{
    ListCell *lc;

    // Make a copy of the original list (don't modify input)
    oldpublist = list_copy(oldpublist);

    // Check for duplicates in the new publication list
    check_duplicates_in_publist(newpublist, NULL);

    // Process each publication in the new list
    foreach(lc, newpublist) {
        char *name = strVal(lfirst(lc));
        ListCell *lc2;
        bool found = false;

        // Search for this publication in the existing list
        foreach(lc2, oldpublist) {
            char *pubname = strVal(lfirst(lc2));

            if (strcmp(name, pubname) == 0) {
                found = true;
                if (addpub) {
                    // Error: trying to add a publication that already exists
                    ereport(ERROR, "publication already in subscription");
                } else {
                    // Remove the publication from the list
                    oldpublist = foreach_delete_current(oldpublist, lc2);
                }
                break;
            }
        }

        // Handle ADD: publication not found, so add it
        if (addpub && !found)
            oldpublist = lappend(oldpublist, makeString(name));
        // Handle DROP: publication not found, that's an error
        else if (!addpub && !found)
            ereport(ERROR, "publication not in subscription");
    }

    // Prevent dropping all publications (leaves subscription invalid)
    if (!oldpublist)
        ereport(ERROR, "cannot drop all publications from subscription");

    return oldpublist;
}
```