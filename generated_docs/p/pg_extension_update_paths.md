# pg_extension_update_paths

## Location
[src/backend/commands/extension.c:2339-2423](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2339-L2423)

## Overview
Reports the version update paths that exist for a specified extension, providing information about how to upgrade or downgrade between different extension versions.

## Definition

```c
Datum
pg_extension_update_paths(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL SQL-callable function that analyzes an extension's control file and script directory to determine all possible version update paths. It returns a set of rows showing the source version, target version, and the path of intermediate versions needed to get from one version to another. The function uses Dijkstra's shortest path algorithm internally to find the most efficient update sequences.

The function reads the extension's control file and extracts version information from available update scripts in the extension's script directory. For each pair of versions, it attempts to find the shortest update path and returns the results as a table with three columns: source version, target version, and the path string showing intermediate steps.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (Name): The name of the extension to analyze for update paths

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NAME: Extracts extension name from function arguments
  - [check_valid_extension_name](../c/check_valid_extension_name.md): Validates extension name format
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md): Initializes set-returning function infrastructure
  - [read_extension_control_file](../r/read_extension_control_file.md): Reads extension control file
  - [get_ext_ver_list](../g/get_ext_ver_list.md): Extracts version information from extension scripts
  - [find_update_path](../f/find_update_path.md): Finds shortest path between two extension versions
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md): Stores result rows in tuple store
- Called from:
  - SQL queries via system function calls (typically invoked as SELECT * FROM pg_extension_update_paths('extension_name'))

## Notes and Other Information
- This is a set-returning function (SRF) that can be called from SQL
- The function validates the extension name before performing any filesystem operations
- Returns NULL for the path column when no update path exists between two versions
- The path string format shows versions connected by '--' (e.g., '1.0--1.1--1.2')
- The function examines all possible version pairs, making it potentially expensive for extensions with many versions
- Located in src/backend/commands/extension.c:2339-2423

## Simplified Source

```c
Datum
pg_extension_update_paths(PG_FUNCTION_ARGS)
{
    Name extname = PG_GETARG_NAME(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    List *evi_list;
    ExtensionControlFile *control;
    ListCell *lc1;

    // Validate extension name and setup
    check_valid_extension_name(NameStr(*extname));
    InitMaterializedSRF(fcinfo, 0);

    // Read extension control file and get version list
    control = read_extension_control_file(NameStr(*extname));
    evi_list = get_ext_ver_list(control);

    // Check all version pairs for update paths
    foreach(lc1, evi_list)
    {
        ExtensionVersionInfo *evi1 = (ExtensionVersionInfo *) lfirst(lc1);
        ListCell *lc2;

        foreach(lc2, evi_list)
        {
            ExtensionVersionInfo *evi2 = (ExtensionVersionInfo *) lfirst(lc2);
            List *path;
            Datum values[3];
            bool nulls[3];

            if (evi1 == evi2)
                continue;

            // Find shortest update path from evi1 to evi2
            path = find_update_path(evi_list, evi1, evi2, false, true);

            memset(values, 0, sizeof(values));
            memset(nulls, 0, sizeof(nulls));

            // Fill result: source, target, path
            values[0] = CStringGetTextDatum(evi1->name);
            values[1] = CStringGetTextDatum(evi2->name);

            if (path == NIL)
                nulls[2] = true;
            else
            {
                // Build path string: "1.0--1.1--1.2"
                StringInfoData pathbuf;
                ListCell *lcv;

                initStringInfo(&pathbuf);
                appendStringInfoString(&pathbuf, evi1->name);
                foreach(lcv, path)
                {
                    char *versionName = (char *) lfirst(lcv);
                    appendStringInfoString(&pathbuf, "--");
                    appendStringInfoString(&pathbuf, versionName);
                }
                values[2] = CStringGetTextDatum(pathbuf.data);
                pfree(pathbuf.data);
            }

            tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
        }
    }

    return (Datum) 0;
}
```