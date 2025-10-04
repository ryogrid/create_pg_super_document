# check_wal_consistency_checking

## Location
[src/backend/access/transam/xlog.c:4627-4711](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4627-L4711)

## Overview
A GUC check hook function that validates the wal_consistency_checking parameter, parsing resource manager names and handling deferred validation for custom resource managers.

## Definition
```c
bool check_wal_consistency_checking(char **newval, void **extra, GucSource source)
```

## Detailed Description
This function validates the wal_consistency_checking GUC parameter, which specifies which resource managers should have consistency checking enabled. The parameter accepts a comma-separated list of resource manager names or the special value 'all'. The function parses this string, validates each resource manager name against known built-in managers, and creates a boolean array indicating which managers should have consistency checking enabled.

For custom resource managers that may not yet be loaded during early startup, the function defers validation until shared_preload_libraries are processed. This allows the system to start up even when custom resource managers are specified but not yet available.

## Parameters / Member Variables
- `newval`: Pointer to the new string value being validated
- `extra`: Pointer to store the resulting boolean array for resource managers
- `source`: Source of the configuration change (GucSource enum)

## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [SplitIdentifierString](../S/SplitIdentifierString.md)
  - GUC_check_errdetail
  - [list_free](../l/list_free.md)
  - [RmgrIdExists](../R/RmgrIdExists.md)
  - [GetRmgr](../G/GetRmgr.md)
  - [guc_malloc](../g/guc_malloc.md)
- Called from (representative examples):
  - GUC system during parameter validation

## Notes and Other Information
- Returns false on syntax errors or unrecognized resource manager names
- The 'all' keyword enables consistency checking for all supported resource managers
- Custom resource managers are handled via deferred validation mechanism
- The extra parameter receives a malloc'd boolean array indexed by resource manager ID
- Only resource managers with rm_mask functions support consistency checking

## Simplified Source

```c
bool
check_wal_consistency_checking(char **newval, void **extra, GucSource source)
{
    char *rawstring;
    List *elemlist;
    ListCell *l;
    bool newwalconsistency[RM_MAX_ID + 1];

    // Initialize boolean array for all resource managers
    MemSet(newwalconsistency, 0, (RM_MAX_ID + 1) * sizeof(bool));

    // Parse the comma-separated list
    rawstring = pstrdup(*newval);
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        GUC_check_errdetail("List syntax is invalid.");
        pfree(rawstring);
        list_free(elemlist);
        return false;
    }

    // Process each resource manager name in the list
    foreach(l, elemlist) {
        char *tok = (char *) lfirst(l);
        int rmid;

        // Handle special 'all' keyword
        if (pg_strcasecmp(tok, "all") == 0) {
            for (rmid = 0; rmid <= RM_MAX_ID; rmid++)
                if (RmgrIdExists(rmid) && GetRmgr(rmid).rm_mask != NULL)
                    newwalconsistency[rmid] = true;
        } else {
            // Look for specific resource manager by name
            bool found = false;
            for (rmid = 0; rmid <= RM_MAX_ID; rmid++) {
                if (RmgrIdExists(rmid) && GetRmgr(rmid).rm_mask != NULL &&
                    pg_strcasecmp(tok, GetRmgr(rmid).rm_name) == 0) {
                    newwalconsistency[rmid] = true;
                    found = true;
                    break;
                }
            }

            // Handle unrecognized names
            if (!found) {
                if (!process_shared_preload_libraries_done) {
                    // Defer validation for custom resource managers
                    check_wal_consistency_checking_deferred = true;
                } else {
                    GUC_check_errdetail("Unrecognized key word: \"%s\".", tok);
                    pfree(rawstring);
                    list_free(elemlist);
                    return false;
                }
            }
        }
    }

    // Clean up and store result
    pfree(rawstring);
    list_free(elemlist);

    // Allocate and return the boolean array
    *extra = guc_malloc(ERROR, (RM_MAX_ID + 1) * sizeof(bool));
    memcpy(*extra, newwalconsistency, (RM_MAX_ID + 1) * sizeof(bool));
    return true;
}
```