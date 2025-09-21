42.7. Event Trigger Functions in PL/Tcl  
---  
[Prev](pltcl-trigger.md "42.6. Trigger Functions in PL/Tcl") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")| Chapter 42. PL/Tcl — Tcl Procedural Language| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pltcl-error-handling.md "42.8. Error Handling in PL/Tcl")  
  
* * *

## 42.7. Event Trigger Functions in PL/Tcl #

Event trigger functions can be written in PL/Tcl. PostgreSQL requires that a function that is to be called as an event trigger must be declared as a function with no arguments and a return type of `event_trigger`. 

The information from the trigger manager is passed to the function body in the following variables: 

`$TG_event`
    

The name of the event the trigger is fired for. 

`$TG_tag`
    

The command tag for which the trigger is fired. 

The return value of the trigger function is ignored. 

Here's a little example event trigger function that simply raises a `NOTICE` message each time a supported command is executed: 
    
    
    CREATE OR REPLACE FUNCTION tclsnitch() RETURNS event_trigger AS $$
      elog NOTICE "tclsnitch: $TG_event $TG_tag"
    $$ LANGUAGE pltcl;
    
    CREATE EVENT TRIGGER tcl_a_snitch ON ddl_command_start EXECUTE FUNCTION tclsnitch();
    

* * *

[Prev](pltcl-trigger.md "42.6. Trigger Functions in PL/Tcl") | [Up](pltcl.md "Chapter 42. PL/Tcl — Tcl Procedural Language")|  [Next](pltcl-error-handling.md "42.8. Error Handling in PL/Tcl")  
---|---|---  
42.6. Trigger Functions in PL/Tcl | [Home](index.md "PostgreSQL 17.5 Documentation")|  42.8. Error Handling in PL/Tcl
