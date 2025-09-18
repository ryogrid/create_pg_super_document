# assign_restrict_nonsystem_relation_kind

## Location
src/backend/tcop/postgres.c: 3752 - 3765

## Overview
A GUC assign hook function that applies the parsed flags from the  configuration parameter to the global restriction settings.

## Definition


## Detailed Description
This function serves as the assign hook for the  GUC parameter. It receives the validated and parsed flags from the corresponding check hook and applies them to the global  variable. This simple assignment function ensures that the restriction settings take effect immediately when the configuration parameter is changed.

## Parameters / Member Variables
- : The new string value of the configuration parameter (unused in this implementation as the parsed data is in extra)
- : Pointer to the parsed flags data prepared by the check hook, cast to an integer pointer containing the restriction flags

## Dependencies
- Functions called/Symbols referenced:
  - restrict_nonsystem_relation_kind: Global variable that stores the active restriction flags
- Called from (representative examples):
  - GUC system (via function pointer in guc_hooks.h)

## Notes and Other Information
- This function works in conjunction with  which validates and parses the input
- The actual validation and parsing logic is handled by the check hook, making this assign function very simple
- The flags stored in the global variable control which types of non-system relations are subject to restrictions
- Part of PostgreSQL's mechanism for controlling access to different types of database objects