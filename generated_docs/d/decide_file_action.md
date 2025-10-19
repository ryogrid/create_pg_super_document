# decide_file_action

## Location
[src/bin/pg_rewind/filemap.c:700-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/filemap.c#L700-L860)

## Overview
Determines the appropriate action to perform on a file during pg_rewind operation by analyzing file existence, type, and content differences between source and target systems.

## Definition
```c
static file_action_t decide_file_action(file_entry_t *entry)
```

## Detailed Description
This function implements the core decision logic for pg_rewind file handling. It examines a file entry containing information about a file's status on both source and target systems and determines what action should be taken. The decision process follows this hierarchy:

1. **Special file handling**: Skip control files and system files that shouldn't be modified
2. **Exclusion filtering**: Remove files matching exclusion patterns from the target
3. **Existence-based decisions**: Handle cases where files exist on only one system
4. **Type compatibility**: Ensure files are of the same type on both systems
5. **Content-based decisions**: For regular files, determine if copying, truncating, or no action is needed

For relation data files, the function implements sophisticated logic to handle size differences:
- If target is smaller than source: copy the missing tail (FILE_ACTION_COPY_TAIL)
- If target is larger than source: truncate to source size (FILE_ACTION_TRUNCATE)  
- If sizes are equal: no action needed (FILE_ACTION_NONE)

## Parameters / Member Variables
- `entry`: Pointer to file_entry_t structure containing file information for both source and target systems

## Dependencies
- Functions called/Symbols referenced:
  - [check_file_excluded](../c/check_file_excluded.md)
  - [keepwal_entry_exists](../k/keepwal_entry_exists.md)
  - pg_log_debug
  - [pg_str_endswith](../p/pg_str_endswith.md)
  - [pg_fatal](../p/pg_fatal.md)
  - FILE_ACTION_* constants
  - FILE_TYPE_* constants
- Called from (representative examples):
  - [decide_file_actions](decide_file_actions.md)

## Notes and Other Information
- This is a static function internal to filemap.c
- The function handles the pg_control file specially, leaving it for later processing
- macOS .DS_Store files are explicitly ignored
- The function includes safety checks and will call pg_fatal for unexpected conditions
- For relation files, the logic assumes that WAL replay will handle block-level changes, so only size differences need to be addressed
- The keepwal hash table is consulted to preserve files needed for recovery

## Simplified Source

```c
static file_action_t decide_file_action(file_entry_t *entry)
{
    const char *path = entry->path;

    // Special files that should never be modified
    if (strcmp(path, "global/pg_control") == 0)
        return FILE_ACTION_NONE;

    // Skip system files like .DS_Store
    if (strstr(path, ".DS_Store") != NULL)
        return FILE_ACTION_NONE;

    // Handle excluded files - remove them from target if they exist
    if (check_file_excluded(path, true))
    {
        if (entry->target_exists)
            return FILE_ACTION_REMOVE;
        else
            return FILE_ACTION_NONE;
    }

    // Handle files missing from one system
    if (!entry->target_exists && entry->source_exists)
    {
        // File exists in source but not target - copy it
        switch (entry->source_type)
        {
            case FILE_TYPE_DIRECTORY:
            case FILE_TYPE_SYMLINK:
                return FILE_ACTION_CREATE;
            case FILE_TYPE_REGULAR:
                return FILE_ACTION_COPY;
            case FILE_TYPE_UNDEFINED:
                pg_fatal("unknown file type for \"%s\"", entry->path);
                break;
        }
    }
    else if (entry->target_exists && !entry->source_exists)
    {
        // File exists in target but not source
        if (keepwal_entry_exists(path))
        {
            pg_log_debug("Not removing file \"%s\" because it is required for recovery", path);
            return FILE_ACTION_NONE;
        }
        return FILE_ACTION_REMOVE;
    }
    else if (!entry->target_exists && !entry->source_exists)
    {
        // File doesn't exist in either system (shouldn't happen)
        Assert(false);
        return FILE_ACTION_NONE;
    }

    // File exists on both systems
    Assert(entry->target_exists && entry->source_exists);

    // Ensure files are of same type
    if (entry->source_type != entry->target_type)
        pg_fatal("file \"%s\" is of different type in source and target", entry->path);

    // Never overwrite PG_VERSION files
    if (pg_str_endswith(entry->path, "PG_VERSION"))
        return FILE_ACTION_NONE;

    // Handle different file types
    switch (entry->source_type)
    {
        case FILE_TYPE_DIRECTORY:
        case FILE_TYPE_SYMLINK:
            return FILE_ACTION_NONE;

        case FILE_TYPE_REGULAR:
            if (!entry->isrelfile)
            {
                // Non-relation file - copy completely
                return FILE_ACTION_COPY;
            }
            else
            {
                // Relation file - handle size differences
                if (entry->target_size < entry->source_size)
                    return FILE_ACTION_COPY_TAIL;
                else if (entry->target_size > entry->source_size)
                    return FILE_ACTION_TRUNCATE;
                else
                    return FILE_ACTION_NONE;
            }
            break;

        case FILE_TYPE_UNDEFINED:
            pg_fatal("unknown file type for \"%s\"", path);
            break;
    }

    pg_fatal("could not decide what to do with file \"%s\"", path);
}
```