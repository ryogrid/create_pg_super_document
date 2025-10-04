# BaseBackupAddTarget

## Location
[src/backend/backup/basebackup_target.c:61-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_target.c#L61-L116)

## Overview
Registers a new base backup target type with the PostgreSQL backup system, allowing server extensions to define custom backup destinations and handlers.

## Definition
```c
void BaseBackupAddTarget(char *name,
                        void *(*check_detail) (char *, char *),
                        bbsink *(*get_sink) (bbsink *, void *))
```

## Detailed Description
This function is designed for use by server extensions to register custom base backup target types. It maintains a global list of available backup target types, where each type is identified by a unique name and provides two function pointers for handling backup operations.

The function first ensures the target list is initialized, then searches for an existing target with the same name. If found, it updates the existing entry with new function pointers. If not found, it creates a new entry and adds it to the global list. All memory allocations are performed in TopMemoryContext to ensure persistence across memory context switches.

## Parameters / Member Variables
- `name`: String identifier for the backup target type (e.g., "client", "server-file")
- `check_detail`: Function pointer that validates and processes target-specific detail strings, returning processed configuration data
- `get_sink`: Function pointer that creates and returns a bbsink object for the actual backup data streaming

## Dependencies
- Functions called/Symbols referenced:
  - [initialize_target_list](../i/initialize_target_list.md)
  - [BaseBackupTargetType](BaseBackupTargetType.md) (struct)
  - [bbsink](../b/bbsink.md) (type)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [pstrdup](../p/pstrdup.md)
  - [lappend](../l/lappend.md)

- Called from (representative examples):
  - No direct callers found (intended for extension use)

## Notes and Other Information
- Intended specifically for server extensions to extend backup functionality
- Uses TopMemoryContext for persistent storage across context switches
- Allows updating existing target types if the same name is registered multiple times
- Part of PostgreSQL's pluggable backup target system introduced for flexible backup destinations
- The function is safe to call multiple times with the same name, updating the handlers each time

## Simplified Source

```c
void BaseBackupAddTarget(char *name,
                        void *(*check_detail)(char *, char *),
                        bbsink *(*get_sink)(bbsink *, void *)) {
    BaseBackupTargetType *newtype;
    MemoryContext oldcontext;
    ListCell *lc;

    // Initialize target list if needed
    if (BaseBackupTargetTypeList == NIL)
        initialize_target_list();

    // Check if target already exists - update if found
    foreach(lc, BaseBackupTargetTypeList) {
        BaseBackupTargetType *ttype = lfirst(lc);
        if (strcmp(ttype->name, name) == 0) {
            ttype->check_detail = check_detail;
            ttype->get_sink = get_sink;
            return;
        }
    }

    // Create new target type in persistent memory context
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    newtype = palloc(sizeof(BaseBackupTargetType));
    newtype->name = pstrdup(name);
    newtype->check_detail = check_detail;
    newtype->get_sink = get_sink;
    BaseBackupTargetTypeList = lappend(BaseBackupTargetTypeList, newtype);
    MemoryContextSwitchTo(oldcontext);
}
```