# ObjectAccessPostCreate

## Location
src/include/catalog/objectaccess.h: 69 - 80

## Overview
ObjectAccessPostCreate is a struct that holds arguments for the OAT_POST_CREATE object access hook event, providing context information about object creation operations to security and logging extensions.

## Definition


## Detailed Description
The ObjectAccessPostCreate struct serves as a parameter container for object access hooks that are triggered after object creation (OAT_POST_CREATE events). It provides essential context information to extensions about whether the object creation was initiated by explicit user operations or by internal PostgreSQL mechanisms.

This distinction is crucial for security and logging extensions because internal object creation (such as toast tables, indexes created due to type changes, or other system-generated objects) may need to be handled differently from user-initiated object creation. Extensions can use this information to apply different security policies, filtering rules, or logging levels.

The struct is passed to object access hook functions to provide context about the nature and origin of the object creation event.

## Parameters / Member Variables
- : Boolean flag indicating whether the object creation is internal to PostgreSQL operations (true) or initiated by user operations (false). Examples of internal creation include toast tables and indexes created due to type changes.

## Dependencies
- Functions called/Symbols referenced: None (this is a data structure)
- Called from (representative examples):
  - [RunObjectPostCreateHook](../R/RunObjectPostCreateHook.md)
  - [RunObjectPostCreateHookStr](../R/RunObjectPostCreateHookStr.md)
  - [accesstype_arg_to_string](../a/accesstype_arg_to_string.md)

## Notes and Other Information
- This struct is specifically used with OAT_POST_CREATE hook events
- The is_internal flag helps extensions distinguish between user-initiated and system-initiated object creation
- Extensions using this struct can implement different handling strategies for internal vs. user operations
- Part of PostgreSQL's object access hook infrastructure for security and audit extensions
- Located in src/include/catalog/objectaccess.h:61-69