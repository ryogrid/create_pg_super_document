# get_ps_display

## Location
src/backend/utils/misc/ps_status.c: 530 - 549

## Overview
Retrieves the current activity portion of the process status display string, returning both the string content and its effective length for systems where the string may not be null-terminated.

## Definition


## Detailed Description
The  function provides read access to the currently active portion of the process status display string. Unlike the full  which includes both fixed prefix information (like program name) and variable activity information, this function returns only the activity part that shows what the current process is doing.

The function handles platform-specific considerations:
- On systems that use argv clobbering (), it checks if  is valid since it might be null
- On systems that use process status display ( is not defined), it calculates the activity portion by subtracting the fixed-size prefix from the total buffer
- On systems without process status support (), it returns an empty string

The function also provides the effective length through an output parameter, which is important because some platforms may not null-terminate the status string.

## Parameters / Member Variables
- : Pointer to an integer that will receive the effective length of the returned string. This is necessary because the returned string may not be null-terminated on some platforms.

## Dependencies
- Functions called/Symbols referenced:
  - Uses global variables: , , 
  - Conditional compilation macros: , 
- Called from (representative examples):
  -  in csvlog.c
  -  in elog.c  
  -  in jsonlog.c
  - Via  macro in ps_status.h

## Notes and Other Information
- Returns a  to prevent accidental modification of the process status buffer
- The returned pointer points directly into the global , not a copy
- On platforms without process status support, always returns an empty string with length 0
- The function is primarily used by logging and monitoring subsystems to include current activity information in log entries
- The caller should not assume the returned string is null-terminated; use the returned length instead