# get_ext_ver_list

## Location
[src/backend/commands/extension.c:1204-1266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1204-L1266)

## Overview
Scans the extension script directory to build a comprehensive list of ExtensionVersionInfo structures representing all available extension versions and their upgrade paths.

## Definition
```c
static List *get_ext_ver_list(ExtensionControlFile *control)
```

## Detailed Description
This function analyzes an extension's script directory to discover all available versions and upgrade paths by parsing script filenames. It creates a graph-like structure where each ExtensionVersionInfo node contains a list of versions reachable in one upgrade step.

The function processes two types of script files:
1. Install scripts: Named 'extname--version.sql' - these define installable versions
2. Update scripts: Named 'extname--fromver--tover.sql' - these define upgrade paths between versions

For each valid script file found, it creates ExtensionVersionInfo structures and links them appropriately to build the complete version dependency graph that will later be used by path-finding algorithms.

## Parameters / Member Variables
- `control`: ExtensionControlFile containing extension metadata including the extension name used for script filename matching

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_script_directory](get_extension_script_directory.md)
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md) 
  - [FreeDir](../F/FreeDir.md)
  - [is_extension_script_filename](../i/is_extension_script_filename.md)
  - [get_ext_ver_info](get_ext_ver_info.md)
  - [pstrdup](../p/pstrdup.md)
  - strrchr
  - strstr
  - [lappend](../l/lappend.md)
- Called from (representative examples):
  - [identify_update_path](../i/identify_update_path.md)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [get_available_versions_for_extension](get_available_versions_for_extension.md)
  - [pg_extension_update_paths](../p/pg_extension_update_paths.md)

## Notes and Other Information
- Static function only used within extension.c module
- Handles both install scripts (single version) and update scripts (version transitions)
- Ignores malformed filenames with more than two '--' separators
- Creates bidirectional relationships in the version graph through the 'reachable' lists
- Essential for building the complete extension version dependency graph used by Dijkstra's algorithm
- Script directory location obtained from extension control file

## Simplified Source

```c
static List *get_ext_ver_list(ExtensionControlFile *control) {
    List *evi_list = NIL;
    int extnamelen = strlen(control->name);
    char *location;
    DIR *dir;
    struct dirent *de;

    // Open extension script directory
    location = get_extension_script_directory(control);
    dir = AllocateDir(location);

    // Scan all files in the directory
    while ((de = ReadDir(dir, location)) != NULL) {
        // Check if it's a valid SQL script file for this extension
        if (!is_extension_script_filename(de->d_name) ||
            strncmp(de->d_name, control->name, extnamelen) != 0 ||
            de->d_name[extnamelen] != '-' ||
            de->d_name[extnamelen + 1] != '-') {
            continue;
        }

        // Extract version name(s) from filename (remove extension name and .sql)
        char *vername = pstrdup(de->d_name + extnamelen + 2);
        *strrchr(vername, '.') = '\0';  // Remove .sql suffix

        char *vername2 = strstr(vername, "--");
        if (!vername2) {
            // Install script: extension_name--version.sql
            ExtensionVersionInfo *evi = get_ext_ver_info(vername, &evi_list);
            evi->installable = true;
            continue;
        }

        // Update script: extension_name--fromver--tover.sql
        *vername2 = '\0';  // Split at separator
        vername2 += 2;     // Point to target version

        // Skip malformed filenames with extra separators
        if (strstr(vername2, "--")) {
            continue;
        }

        // Create version info and link them for update path
        ExtensionVersionInfo *evi = get_ext_ver_info(vername, &evi_list);
        ExtensionVersionInfo *evi2 = get_ext_ver_info(vername2, &evi_list);
        evi->reachable = lappend(evi->reachable, evi2);
    }

    FreeDir(dir);
    return evi_list;
}
```