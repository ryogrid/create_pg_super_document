# InitializeWalConsistencyChecking

## Location
[src/backend/access/transam/xlog.c:4739-4764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4739-L4764)

## Overview
A startup function that processes deferred wal_consistency_checking validation after shared_preload_libraries are loaded, ensuring custom resource managers are properly recognized.

## Definition
```c
void InitializeWalConsistencyChecking(void)
```

## Detailed Description
This function is called during server startup after shared_preload_libraries have been loaded to handle any deferred validation of the wal_consistency_checking parameter. During early startup, if unknown resource managers were specified in wal_consistency_checking, validation was deferred because custom resource managers might not yet be loaded. This function re-processes the wal_consistency_checking configuration to properly validate and assign any previously unknown resource manager names.

The function uses the GUC system to re-set the wal_consistency_checking parameter, which triggers the full validation and assignment process again. This ensures that custom resource managers that are now loaded can be properly recognized and configured for consistency checking.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [find_option](../f/find_option.md)
  - [set_config_option_ext](../s/set_config_option_ext.md)
  - [config_generic](../c/config_generic.md) (struct)
  - GUC_ACTION_SET
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [PostgresSingleUserMain](../P/PostgresSingleUserMain.md)

## Notes and Other Information
- Must be called after process_shared_preload_libraries_done is true
- Only performs work if check_wal_consistency_checking_deferred flag is set
- Re-triggers the full GUC validation and assignment process
- Ensures that all resource manager names are validated after modules are loaded
- Contains assertions to verify proper state transitions

## Simplified Source

```c
// Simplified version of InitializeWalConsistencyChecking
void InitializeWalConsistencyChecking(void) {
    // Verify we're called after shared libraries are loaded
    Assert(process_shared_preload_libraries_done);

    // Check if deferred validation is needed
    if (check_wal_consistency_checking_deferred) {
        // Find the wal_consistency_checking GUC option
        struct config_generic *guc = find_option("wal_consistency_checking", false, false, ERROR);

        // Clear the deferred flag before re-processing
        check_wal_consistency_checking_deferred = false;

        // Re-process the wal_consistency_checking parameter with full validation
        // This will now recognize any custom resource managers that were loaded
        set_config_option_ext("wal_consistency_checking",
                              wal_consistency_checking_string,
                              guc->scontext, guc->source, guc->srole,
                              GUC_ACTION_SET, true, ERROR, false);

        // Verify deferred flag remains clear after processing
        Assert(!check_wal_consistency_checking_deferred);
    }
}
```

Key simplifications made:
- Added clear comments explaining each logical step
- Maintained the essential assertion checks for safety
- Preserved the exact control flow and function calls
- Made the deferred validation logic more explicit with comments
- Kept the original parameter passing structure for set_config_option_ext