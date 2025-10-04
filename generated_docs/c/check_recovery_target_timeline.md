# check_recovery_target_timeline

## Location
[src/backend/access/transam/xlogrecovery.c:4966-4998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L4966-L4998)

## Overview
A GUC check hook function that validates and processes recovery_target_timeline parameter values, supporting both special keywords and numeric timeline IDs.

## Definition

```c
bool
check_recovery_target_timeline(char **newval, void **extra, GucSource source)
```
## Detailed Description
This function serves as a GUC check hook for the  parameter. It validates the input value and converts it into an appropriate  enum value. The function accepts three types of values: "current" (uses control file timeline), "latest" (uses the latest available timeline), and numeric values (specific timeline ID). For numeric values, it performs validation using  to ensure the value is a valid number. The processed goal type is stored in the  parameter for later use by the assign hook.

## Parameters / Member Variables
- `**newval`: Pointer to the new value string being assigned to recovery_target_timeline
- `**extra`: Pointer to store additional processed data (RecoveryTargetTimeLineGoal enum)
- `source`: The source of the GUC setting (configuration file, command line, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - strtoul (for numeric validation)
  - GUC_check_errdetail (for error reporting)
  - [guc_malloc](../g/guc_malloc.md) (for memory allocation)
  - RECOVERY_TARGET_TIMELINE_CONTROLFILE (enum value)
  - RECOVERY_TARGET_TIMELINE_LATEST (enum value)
  - RECOVERY_TARGET_TIMELINE_NUMERIC (enum value)
- Called from (representative examples):
  - GUC system (via function pointer in GUC_HOOKS_H)

## Notes and Other Information
- This function is part of PostgreSQL's point-in-time recovery (PITR) system
- Supports three timeline recovery modes: current (from control file), latest (newest available), and specific numeric timeline
- Returns false if the numeric value is invalid, causing the GUC assignment to fail
- Allocates memory for the RecoveryTargetTimeLineGoal enum to pass to the assign hook
- Located in src/backend/access/transam/xlogrecovery.c:4966-4998

## Simplified Source

```c
bool check_recovery_target_timeline(char **newval, void **extra, GucSource source) {
    RecoveryTargetTimeLineGoal rttg;

    // Parse timeline specification
    if (strcmp(*newval, "current") == 0)
        rttg = RECOVERY_TARGET_TIMELINE_CONTROLFILE;
    else if (strcmp(*newval, "latest") == 0)
        rttg = RECOVERY_TARGET_TIMELINE_LATEST;
    else {
        // Validate numeric timeline ID
        rttg = RECOVERY_TARGET_TIMELINE_NUMERIC;
        errno = 0;
        strtoul(*newval, NULL, 0);
        if (errno == EINVAL || errno == ERANGE) {
            GUC_check_errdetail("\"recovery_target_timeline\" is not a valid number.");
            return false;
        }
    }

    // Store parsed goal for assign hook
    RecoveryTargetTimeLineGoal *myextra = (RecoveryTargetTimeLineGoal *)
        guc_malloc(ERROR, sizeof(RecoveryTargetTimeLineGoal));
    *myextra = rttg;
    *extra = (void *) myextra;

    return true;
}
```

**Simplified Logic:**
1. **Parse Keywords**: Recognizes "current" and "latest" special timeline values
2. **Numeric Validation**: For other values, validates as numeric timeline IDs using strtoul
3. **Error Handling**: Returns false with detailed error message for invalid numbers
4. **Store Goal**: Allocates memory to store the timeline goal enum for the assign hook