# LoadOutputPlugin

## Location
[src/backend/replication/logical/logical.c:752-773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L752-L773)

## Overview
LoadOutputPlugin is a static function that loads an external logical replication output plugin, initializes it, and validates that it provides all required callback functions.

## Definition

```c
struct */
	plugin_init(callbacks);
```
## Detailed Description
This function is responsible for dynamically loading logical replication output plugins and ensuring they conform to PostgreSQL's output plugin interface. It loads the plugin's shared library, calls the plugin's initialization function, and validates that all mandatory callbacks are registered. The function serves as a critical validation step in the logical replication setup process, ensuring that plugins are properly implemented before they can be used for logical decoding.

The function follows a strict validation protocol where it checks for the presence of three essential callbacks (begin_cb, change_cb, and commit_cb) that are required for basic logical replication functionality.

## Parameters / Member Variables
- : Pointer to OutputPluginCallbacks structure that will be filled by the plugin's initialization function
- : Name of the output plugin to load (used to locate the shared library)

## Dependencies
- Functions called/Symbols referenced:
  - [load_external_function](../l/load_external_function.md)
  - [OutputPluginCallbacks](../O/OutputPluginCallbacks.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- The function expects plugins to export a  symbol as their entry point
- Three callbacks are mandatory: begin_cb, change_cb, and commit_cb
- The function will terminate with ERROR if the plugin doesn't provide required callbacks
- This is a static function only used within the logical replication subsystem
- The plugin loading mechanism uses PostgreSQL's dynamic loading infrastructure

## Simplified Source

```c
static void LoadOutputPlugin(OutputPluginCallbacks *callbacks, const char *plugin)
{
    LogicalOutputPluginInit plugin_init;

    // Load the plugin's initialization function
    plugin_init = (LogicalOutputPluginInit)
        load_external_function(plugin, "_PG_output_plugin_init", false, NULL);

    if (plugin_init == NULL)
        elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");

    // Initialize the plugin and let it fill the callback structure
    plugin_init(callbacks);

    // Validate that required callbacks are present
    if (callbacks->begin_cb == NULL)
        elog(ERROR, "output plugins have to register a begin callback");
    if (callbacks->change_cb == NULL)
        elog(ERROR, "output plugins have to register a change callback");
    if (callbacks->commit_cb == NULL)
        elog(ERROR, "output plugins have to register a commit callback");
}
```