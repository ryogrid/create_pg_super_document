# OutputPluginOptions

## Location
src/include/replication/output_plugin.h: 26 - 30

## Overview
OutputPluginOptions is a structure that defines configuration options set by logical replication output plugins during their startup callback to control decoding behavior.

## Definition


## Detailed Description
OutputPluginOptions serves as a configuration structure that logical replication output plugins use to communicate their preferences and capabilities to the PostgreSQL logical decoding infrastructure. This structure is populated by output plugins during their startup callback function and instructs the logical decoding system how to format and deliver change data to the plugin.

The structure allows plugins to specify whether they want binary or textual output format and whether they are capable of handling rewritten transactions. This configuration affects how the logical decoding system processes and delivers change records to the plugin.

## Parameters / Member Variables
- : Specifies the desired output format using OutputPluginOutputType enum (OUTPUT_PLUGIN_BINARY_OUTPUT or OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
- : Boolean flag indicating whether the plugin can handle rewritten transactions (transactions that have been modified by triggers or rules)

## Dependencies
- Functions called/Symbols referenced:
  - OutputPluginOutputType
- Called from (representative examples):
  - startup_cb_wrapper (src/backend/replication/logical/logical.c:793)
  - [pgoutput_startup](../p/pgoutput_startup.md) (src/backend/replication/pgoutput/pgoutput.c:434)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (src/include/replication/logical.h:54)

## Notes and Other Information
This structure is typically initialized and configured in the LogicalDecodeStartupCB callback function implemented by output plugins. The settings in this structure affect the entire lifetime of a logical decoding session and cannot be changed after the startup phase. Output plugins must carefully consider their capabilities when setting these options, as they determine the format and type of data the plugin will receive from the logical decoding system.