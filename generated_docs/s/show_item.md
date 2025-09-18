# show_item

## Location
src/bin/pg_config/pg_config.c: 116 - 129

## Overview
Searches through configuration data array and displays the value of a specified configuration parameter.

## Definition
```c
static void show_item(const char *configname, ConfigData *configdata, size_t configdata_len)
```

## Detailed Description
The show_item function performs a linear search through a ConfigData array to find a configuration parameter matching the specified name. When a match is found, it prints the corresponding configuration value to stdout. This function is part of the pg_config utility's mechanism for displaying specific PostgreSQL build-time configuration values. The search is case-sensitive and uses string comparison to match parameter names exactly.

## Parameters / Member Variables
- `configname`: The name of the configuration parameter to search for and display
- `configdata`: Pointer to an array of ConfigData structures containing configuration name-value pairs
- `configdata_len`: The number of elements in the configdata array, used to limit the search bounds

## Dependencies
- Functions called/Symbols referenced:
  - ConfigData (structure type for configuration data)
  - strcmp (standard C library string comparison function)
  - printf (standard C library output function)
- Called from (representative examples):
  - main (src/bin/pg_config/pg_config.c:174)

## Notes and Other Information
- Performs a simple linear search algorithm with O(n) complexity
- Only prints the first matching configuration item found
- Does not provide error handling if the configuration parameter is not found
- Part of the pg_config utility which exposes PostgreSQL build configuration information
- Used to extract specific configuration values rather than displaying all available options
- The function assumes the ConfigData structure has 'name' and 'setting' string fields