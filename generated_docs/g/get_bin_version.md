# get_bin_version

## Location
src/bin/pg_upgrade/exec.c: 33 - 84

## Overview
This function fetches the major version number of PostgreSQL binaries for a given cluster by executing pg_ctl and parsing its version output.

## Definition


## Detailed Description
The  function determines the PostgreSQL version of the binaries in a cluster's bin directory. It executes the  command and parses the output to extract version numbers. The function handles both old-style versioning (e.g., 9.6.1) where major version is v1 and minor is v2, and new-style versioning (e.g., 10.1) where v1 is the complete major version number. The parsed version is stored as an integer in the cluster's bin_version field using the formula: major * 10000 + minor * 100 for old style, or major * 10000 for new style.

## Parameters / Member Variables
- : Pointer to ClusterInfo structure that will have its bin_version field populated with the detected version number

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - fflush
  - popen
  - fgets
  - pclose
  - pg_fatal
  - wait_result_to_str
  - sscanf
- Called from (representative examples):
  - check_bin_dir

## Notes and Other Information
- This is a static function, only accessible within the exec.c file
- The function uses popen to execute shell commands, making it platform-dependent
- Version parsing logic handles the PostgreSQL version numbering change that occurred with version 10
- Error handling includes checks for command execution failure and version parsing failure
- The function modifies the bin_version field of the provided ClusterInfo structure