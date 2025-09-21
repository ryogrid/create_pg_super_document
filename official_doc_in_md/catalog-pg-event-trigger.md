51.21. `pg_event_trigger`  
---  
[Prev](catalog-pg-enum.md "51.20. pg_enum") | [Up](catalogs.md "Chapter 51. System Catalogs")| Chapter 51. System Catalogs| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](catalog-pg-extension.md "51.22. pg_extension")  
  
* * *

## 51.21. `pg_event_trigger` #

The catalog `pg_event_trigger` stores event triggers. See [Chapter 38](event-triggers.md "Chapter 38. Event Triggers") for more information. 

**Table 51.21.`pg_event_trigger` Columns**

Column Type  Description   
---  
`oid` `oid` Row identifier   
`evtname` `name` Trigger name (must be unique)   
`evtevent` `name` Identifies the event for which this trigger fires   
`evtowner` `oid` (references [`pg_authid`](catalog-pg-authid.md "51.8. pg_authid").`oid`)  Owner of the event trigger   
`evtfoid` `oid` (references [`pg_proc`](catalog-pg-proc.md "51.39. pg_proc").`oid`)  The function to be called   
`evtenabled` `char` Controls in which [session_replication_role](runtime-config-client.md#GUC-SESSION-REPLICATION-ROLE) modes the event trigger fires. `O` = trigger fires in “origin” and “local” modes, `D` = trigger is disabled, `R` = trigger fires in “replica” mode, `A` = trigger fires always.   
`evttags` `text[]` Command tags for which this trigger will fire. If NULL, the firing of this trigger is not restricted on the basis of the command tag.   
  
  


* * *

[Prev](catalog-pg-enum.md "51.20. pg_enum") | [Up](catalogs.md "Chapter 51. System Catalogs")|  [Next](catalog-pg-extension.md "51.22. pg_extension")  
---|---|---  
51.20. `pg_enum` | [Home](index.md "PostgreSQL 17.5 Documentation")|  51.22. `pg_extension`
