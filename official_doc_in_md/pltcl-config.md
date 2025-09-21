42.11. PL/Tcl Configuration  
---  
[Prev](pltcl-transactions.md "42.10. Transaction Management") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")| Chapter 42. PL/Tcl — Tcl Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pltcl-procnames.md "42.12. Tcl Procedure Names")  
  
* * *

## 42.11. PL/Tcl Configuration #

This section lists configuration parameters that affect PL/Tcl. 

`pltcl.start_proc` (`string`)  #
    

This parameter, if set to a nonempty string, specifies the name (possibly schema-qualified) of a parameterless PL/Tcl function that is to be executed whenever a new Tcl interpreter is created for PL/Tcl. Such a function can perform per-session initialization, such as loading additional Tcl code. A new Tcl interpreter is created when a PL/Tcl function is first executed in a database session, or when an additional interpreter has to be created because a PL/Tcl function is called by a new SQL role. 

The referenced function must be written in the `pltcl` language, and must not be marked `SECURITY DEFINER`. (These restrictions ensure that it runs in the interpreter it's supposed to initialize.) The current user must have permission to call it, too. 

If the function fails with an error it will abort the function call that caused the new interpreter to be created and propagate out to the calling query, causing the current transaction or subtransaction to be aborted. Any actions already done within Tcl won't be undone; however, that interpreter won't be used again. If the language is used again the initialization will be attempted again within a fresh Tcl interpreter. 

Only superusers can change this setting. Although this setting can be changed within a session, such changes will not affect Tcl interpreters that have already been created. 

`pltclu.start_proc` (`string`)  #
    

This parameter is exactly like `pltcl.start_proc`, except that it applies to PL/TclU. The referenced function must be written in the `pltclu` language. 

* * *

[Prev](pltcl-transactions.md "42.10. Transaction Management") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")|  [Next](pltcl-procnames.md "42.12. Tcl Procedure Names")  
---|---|---  
42.10. Transaction Management | [Home](index.md "PostgreSQL 17.5 Documentation")|  42.12. Tcl Procedure Names
