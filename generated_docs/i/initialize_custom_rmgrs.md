# initialize_custom_rmgrs

## Location
src/bin/pg_waldump/rmgrdesc.c: 73 - 86

## Overview
Initializes descriptors for custom resource managers with default names and fallback functions when custom modules are not loaded.

## Definition
```c
static void initialize_custom_rmgrs(void)
```

## Detailed Description
The `initialize_custom_rmgrs` function sets up resource manager descriptors for custom resource managers in pg_waldump. Since custom resource manager modules are not loaded in pg_waldump, this function generates generic descriptors with numeric names in the format "custom###" (where ### is the 3-digit resource manager ID). Each descriptor is assigned default description and identification functions that provide minimal information about custom resource manager records.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - RM_N_CUSTOM_IDS (constant defining number of custom resource manager slots)
  - CUSTOM_NUMERIC_NAME_LEN (constant defining length of numeric names)
  - RM_MIN_CUSTOM_ID (constant defining the minimum custom resource manager ID)
  - default_desc (assigned as the description function)
  - default_identify (assigned as the identification function)
  - RmgrDescData (structure type used for resource manager descriptors)
- Called from:
  - GetRmgrDesc (called to initialize custom resource managers when first needed)

## Notes and Other Information
- This function is static and only used within rmgrdesc.c
- Sets the global CustomRmgrDescInitialized flag to true after initialization
- Creates generic names like "custom128", "custom129", etc. for custom resource managers
- The function populates the CustomRmgrDesc array and CustomNumericNames array
- Part of the pg_waldump utility's resource manager handling system
- Only called once during the lifetime of the program (lazy initialization)