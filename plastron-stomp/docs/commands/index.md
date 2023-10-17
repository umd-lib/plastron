# STOMP Commands

Command functionality for the STOMP daemon is implemented by *command 
functions*, found in *command modules* under the `plastron.stomp.commands` 
package.

The command function **MUST** have the same name as its command module.

A command function takes three arguments:

* a `plastron.repo.Repository` instance, representing the target repository
* a dictionary of command-specific configuration information
* a `plastron.stomp.messages.PlastronCommandMessage` instance, 
  representing the command to be executed

A command function **MUST** return a generator with these characteristics:

* yield type is `Dict[str, Any]`; this is used to convey progress 
  information back to the STOMP command processor
* send type is `None`; the generator does not accept `send()` calls
* return type is `Dict[str, Any]`; this represents the final status of the 
  system after running the command

Here is a simple function that indicates its progress counting up to a given 
number:

```python
# ignoring the repository and configuration arguments in this simple example
def count_up(_repo, _config, message):
    target = int(message.args.get('target', 10))
    # count from 1 to target, inclusive
    for n in range(1, target + 1):
        # progress message
        yield {'target': target, 'current': n}
    return {'type': 'Done', 'message': f'Counted up to {target}!'}
```

## List of Plastron STOMP Commands

* echo
* export
* import
* update
