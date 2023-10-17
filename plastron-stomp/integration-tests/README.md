# Manual Integration Tests for STOMP Commands

## Prerequisites

* a running fcrepo Docker stack
* the [netcat] (`nc`) command line tool
  * Mac users can install via Homebrew: `brew install netcat`

### Required Configuration

_fcrepo-local.yml_

```yaml
REPOSITORY:
   REST_ENDPOINT: http://fcrepo-local:8080/fcrepo/rest
   RELPATH: /
   AUTH_TOKEN: ... # JWT token generated from http://fcrepo-local:8080/fcrepo/user
   LOG_DIR: logs
   STRUCTURE: hierarchical
MESSAGE_BROKER:
   SERVER: localhost:61613
   MESSAGE_STORE_DIR: msg
   DESTINATIONS:
      JOBS: /queue/plastron.jobs
      JOB_STATUS: /queue/plastron.job.status
      JOB_PROGRESS: /queue/plastron.job.progress
      SYNCHRONOUS_JOBS: /queue/plastron.jobs.synchronous
      REINDEXING: /queue/reindex
```

## Steps

1. Start up the Plastron STOMP daemon:
   ```bash
   python -m plastron.stomp.daemon -c fcrepo-local.yml
   ```
2. Remove any prior _test-import_ job directory:
   ```bash
   rm -rf jobs/test-import
   ```
3. Change into the integration-tests directory from the main Plastron 
   project directory:
   ```bash
   cd plastron-stomp/integration-tests
   ```
4. Send the import command:
   ```bash
   nc localhost 61613 <stomp-import
   ```
5. Confirm that there is an imported item at 
   <http://fcrepo-local:8080/fcrepo/rest/import-test>
6. Send the update command:
   ```bash
   nc localhost 61613 <stomp-update
   ```
7. Confirm that the previously imported item at
   <http://fcrepo-local:8080/fcrepo/rest/import-test>
   now also has a `dcterms:title` value of "Moonpig"
8. Send the export command:
   ```bash
   nc localhost 61613 <stomp-export
   ```
9. Verify that there is an exported _test-export.tar.gz_
   file in the current directory. List the contents using
   `tar tzf test-export.tar.gz` and you should see:
   ```
   test-export.tar/bagit.txt
   test-export.tar/bag-info.txt
   test-export.tar/manifest-sha512.txt
   test-export.tar/tagmanifest-sha256.txt
   test-export.tar/tagmanifest-sha512.txt
   test-export.tar/manifest-sha256.txt
   test-export.tar/data/metadata.ttl
   test-export.tar/data/b4a968dd-dbc6-42a0-9799-760266d44cd4/labor-087802-0001.tif
   ```

## Editing the Test Files

The _stomp-*_ test files are text files containing the raw STOMP message 
sequences that are sent to the STOMP server, and must follow all the rules
of the STOMP wire protocol. In particular, each message (or "frame", in
STOMP parlance) must end with a NULL character (codepoint 0x00). This
can be problematic for some text editors to read, write, or even
display.

### Editor Support

| Editor  | Support? | Display | Input                                                             |
|---------|----------|---------|-------------------------------------------------------------------|
| Vim     | Yes      | `^@`    | <kbd>Ctrl</kbd>+<kbd>V</kbd> <kbd>x</kbd><kbd>0</kbd><kbd>0</kbd> |
| VS Code | Partial  | `␀`     | _Unknown_                                                         |
| PyCharm | No       | —       | —                                                                 |

[netcat]: https://en.wikipedia.org/wiki/Netcat
