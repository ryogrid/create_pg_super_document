# drop_descriptor

## Location
[src/interfaces/ecpg/preproc/descriptor.c:99-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/descriptor.c#L99-L130)

## Overview
Removes a specific SQL descriptor from the global descriptors linked list, matching both name and connection criteria, with error reporting for non-existent descriptors.

## Definition

```c
struct descriptor *i;
```
## Detailed Description
This function searches for and removes a descriptor from the global descriptors linked list. It performs an exact match on both the descriptor name and connection string. The function validates that the name starts with a quote character before processing and handles both connected and default (NULL connection) descriptors.

The function implements a safe linked list removal by maintaining a pointer to the previous node's next pointer. If the descriptor is found and matches the connection criteria, it's removed and its memory is freed. If no matching descriptor is found, an appropriate warning message is generated.

## Parameters / Member Variables
- : The name of the descriptor to remove (must start with '"')
- : The connection string to match (can be NULL for default connection)

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (string comparison function)
  - free (memory deallocation function)
  - mmerror (error reporting function)
  - [descriptor](descriptor.md) (struct type)
  - PARSE_ERROR, ET_WARNING (error reporting constants)
- Called from (representative examples):
  - Grammar rules in ecpg.trailer for descriptor cleanup

## Notes and Other Information
- Returns early if the name doesn't start with a quote character (\")
- Handles both NULL and non-NULL connection parameters correctly
- Uses a safe linked list removal technique with double pointer for previous node
- Frees all allocated memory: connection string, name string, and descriptor structure
- Generates specific warning messages for missing descriptors, distinguishing between default and named connections
- Part of the ECPG preprocessor's descriptor lifecycle management system
- The error messages use ET_WARNING rather than ET_ERROR, indicating non-fatal issues

## Simplified Source

```c
void drop_descriptor(char *name, char *connection) {
    // Validate name format
    if (name[0] != '"')
        return;

    // Search for matching descriptor
    struct descriptor **lastptr = &descriptors;
    for (struct descriptor *i = descriptors; i; lastptr = &i->next, i = i->next) {
        if (strcmp(name, i->name) == 0) {
            // Check connection match (both NULL or both equal)
            bool connection_matches = (!connection && !i->connection) ||
                                    (connection && i->connection &&
                                     strcmp(connection, i->connection) == 0);

            if (connection_matches) {
                // Remove node and free memory
                *lastptr = i->next;
                free(i->connection);
                free(i->name);
                free(i);
                return;
            }
        }
    }

    // Report error if not found
    if (connection)
        mmerror(PARSE_ERROR, ET_WARNING, "descriptor %s bound to connection %s does not exist", name, connection);
    else
        mmerror(PARSE_ERROR, ET_WARNING, "descriptor %s bound to the default connection does not exist", name);
}
```