# ObjectAccessDrop

## Location
src/include/catalog/objectaccess.h: 81 - 102

## Overview
ObjectAccessDrop is a struct that holds arguments for the OAT_DROP object access hook event, providing context information about object deletion operations to security and logging extensions.

## Definition


## Detailed Description
The ObjectAccessDrop struct serves as a parameter container for object access hooks that are triggered before object deletion (OAT_DROP events). It provides essential context information to extensions about the nature and circumstances of the object deletion operation.

The primary purpose is to inform extensions about the deletion context through flags that correspond to the PERFORM_DELETION_* constants defined in dependency.h. These flags help extensions understand whether the deletion is part of a cascade operation, a restricted deletion, or other specific deletion scenarios.

This information enables security and logging extensions to make informed decisions about how to handle different types of deletion operations, potentially applying different policies or logging levels based on the deletion context.

## Parameters / Member Variables
- : Integer flags that inform extensions about the context of the deletion operation. These flags correspond to PERFORM_DELETION_* constants in dependency.h, providing details about whether the deletion is cascaded, restricted, or has other special characteristics.

## Dependencies
- Functions called/Symbols referenced: 
  - PERFORM_DELETION_* constants (from dependency.h)
- Called from (representative examples):
  - RunObjectDropHook
  - RunObjectDropHookStr
  - accesstype_arg_to_string

## Notes and Other Information
- This struct is specifically used with OAT_DROP hook events
- The dropflags field allows extensions to distinguish between different deletion contexts (cascade, restrict, etc.)
- Extensions can use this information to implement appropriate security checks or logging for different deletion scenarios
- Part of PostgreSQL's object access hook infrastructure for security and audit extensions
- The flags reference PERFORM_DELETION_* constants defined in dependency.h for deletion behavior control
- Located in src/include/catalog/objectaccess.h:74-81