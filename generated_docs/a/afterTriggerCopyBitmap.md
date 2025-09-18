# afterTriggerCopyBitmap

## Location
src/backend/commands/trigger.c: 4086 - 4110

## Overview
Copies a Bitmapset into the AfterTriggerEvents memory context to ensure proper memory management during after-trigger event processing.

## Definition
```c
static Bitmapset *afterTriggerCopyBitmap(Bitmapset *src)
```

## Detailed Description
This function creates a deep copy of a Bitmapset in the AfterTriggerEvents memory context. It temporarily switches to the afterTriggers.event_cxt memory context, performs the bitmap copy operation, and then switches back to the original context. This ensures that bitmap data used by after-trigger events is allocated in the appropriate long-lived memory context and will persist for the duration of trigger event processing. The function handles NULL input gracefully by returning NULL.

## Parameters / Member Variables
- `src`: The source Bitmapset to be copied. Can be NULL, in which case the function returns NULL.

## Dependencies
- Functions called/Symbols referenced:
  - bms_copy
  - MemoryContextSwitchTo (implicit)
  - afterTriggers.event_cxt (global variable access)
- Called from (representative examples):
  - Used in trigger event processing contexts where bitmaps need to be preserved

## Notes and Other Information
- The function is static, indicating it is only used within the trigger.c module
- Memory context switching ensures the copied bitmap persists in the correct context
- This is part of PostgreSQL's after-trigger event management system
- The function follows PostgreSQL's pattern of careful memory context management