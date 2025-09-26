# getrusage

## Location
src/port/win32getrusage.c: 21 - 61

## Overview
A Windows-specific implementation of the POSIX  function that retrieves resource usage information for the current process, providing user and system CPU time statistics.

## Definition


## Detailed Description
This function is PostgreSQL's Windows port implementation of the standard POSIX  system call. Since Windows doesn't provide a native  function, this implementation uses Windows-specific APIs () to gather equivalent resource usage information.

The function specifically provides CPU time usage statistics by converting Windows  structures (which represent time in 100-nanosecond intervals) to the standard  format used by POSIX .

The implementation is limited to retrieving information about the current process () and does not support child process statistics (), which aligns with PostgreSQL's typical usage patterns on Windows.

## Parameters / Member Variables
- : Specifies which processes to return information about. Currently only  (0) is supported for the current process
- : Pointer to a  structure where the resource usage information will be stored

## Dependencies
- Functions called/Symbols referenced:
  -  - Windows API to retrieve process timing information
  -  - Windows API to get handle to current process
  -  - Windows API to get last error code
  -  - Maps Windows error codes to errno values
  -  - Standard C library function to zero-initialize memory
  -  - Standard C library function to copy memory
  -  - Constant defined as 0
  -  - Structure containing resource usage fields

- Called from (representative examples):
  -  in 
  -  in 
  -  in 

## Notes and Other Information
- This is a Windows-only implementation located in the port layer ()
- Only supports ; attempting to use  returns 
- Converts Windows  (100-nanosecond units) to microseconds for 
- Returns 0 on success, -1 on error with  set appropriately
- The  is simplified on Windows, containing only  (user CPU time) and  (system CPU time)
- Other resource usage fields typically found in full POSIX  implementations (like memory usage) are not populated
- Used primarily by PostgreSQL's performance monitoring and logging infrastructure to track CPU usage