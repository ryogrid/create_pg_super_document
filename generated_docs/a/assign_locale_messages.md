# assign_locale_messages

## Location
[src/backend/utils/adt/pg_locale.c:450-466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L450-L466)

## Overview
This function applies a validated LC_MESSAGES locale setting to the system, serving as a GUC assign hook that actually sets the locale after validation has passed.

## Definition


## Detailed Description
The  function is the assignment counterpart to  in PostgreSQL's GUC system. After a locale value has been validated by the check hook, this assign hook is responsible for actually applying the setting to the system. The function is designed to be fault-tolerant:

1. **Platform compatibility**: Only attempts to set the locale on systems that support LC_MESSAGES
2. **Failure tolerance**: Ignores setlocale failures, as indicated by the comment referencing failure handling policy
3. **Global setting**: Unlike other locale categories, LC_MESSAGES is allowed to be set globally in PostgreSQL

The function uses  which is PostgreSQL's wrapper around the standard  function, providing additional error handling and consistency.

## Parameters / Member Variables
- : The validated locale string to be applied to the system
- : Additional data from the check hook (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_perm_setlocale](../p/pg_perm_setlocale.md) (PostgreSQL's setlocale wrapper function)
- Called from (representative examples):
  - GUC system assignment hooks (referenced in guc_hooks.h)

## Notes and Other Information
- This function is part of the GUC (Grand Unified Configuration) hook system
- Failures in locale setting are intentionally ignored for robustness
- Only compiled and executed on platforms that support LC_MESSAGES category
- Works in conjunction with  to provide complete locale validation and assignment
- The function allows LC_MESSAGES to be set globally, which is unique among PostgreSQL locale settings