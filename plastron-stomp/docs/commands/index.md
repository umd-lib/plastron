# STOMP Commands

Command functionality for the STOMP daemon is implemented by *command 
functions*, found in *command modules* under the `plastron.stomp.commands` 
package.

## Command Functions

The command function **MUST** have the same name as its command module.

A command function takes two arguments:

* a `plastron.context.PlastronContext` instance, representing the 
  execution context of the command (including the repository and client 
  configuration, configuration for external services such as the handle 
  service, and command-specific configuration)
* a `plastron.stomp.messages.PlastronCommandMessage` instance, 
  representing the command to be executed

A command function **MUST** return a generator with these characteristics:

* yield type is `Dict[str, Any]`; this is used to convey progress 
  information back to the STOMP command processor
* send type is `None`; the generator does not accept `send()` calls
* return type is `Dict[str, Any]`; this represents the final status of the 
  system after running the command

Here is a command function that indicates its progress counting up to a given 
number, where the step size is stored in the configuration:

```python
from typing import Any, Dict, Generator
from plastron.context import PlastronContext
from plastron.stomp.messages import PlastronCommandMessage

def count_up(context: PlastronContext, message: PlastronCommandMessage) -> Generator[Dict[str, Any], None, Dict[str, Any]]:
    target = int(message.args.get('target', 10))
    step = context.config['COMMANDS']['COUNT_UP']['STEP_SIZE']
    # count from 1 to target, inclusive
    for n in range(1, target + 1, step):
        # progress message
        yield {'target': target, 'current': n}
    return {
      'type': 'count_up_done',
      'message': f'Counted up to {target} by {step}!',
    }
```

## List of Plastron STOMP Commands

* echo
* export
* import
* publish
* unpublish
* update

## Jobs

All the commands (except `echo`) use *jobs* to encapsulate much of the actual 
work being done by the command, in a way that is independent of the fact 
that the command is running via STOMP.

These job classes are all contained in the `plastron.jobs` module, in the 
[plastron-repo](../../../plastron-repo/README.md) distribution package.
