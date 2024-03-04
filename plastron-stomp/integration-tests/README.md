# Manual Integration Tests for STOMP Commands

## Prerequisites

* a running fcrepo Docker stack
* a running [umd-handle] server at <http://handle-local:3000>

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
PUBLICATION_WORKFLOW:
  HANDLE_ENDPOINT: http://handle-local:3000/api/v1
  HANDLE_JWT_TOKEN: ... # JWT token generated from the umd-handle application
  HANDLE_PREFIX: 1903.1
  HANDLE_REPO: fcrepo
  PUBLIC_URL_PATTERN: http://digital-local/{uuid}
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
4. Enable the integration tests:
   ```bash
   export INTEGRATION_TESTS=1
   ```
5. Send the import command:
   ```bash
   pytest test_import.py
   ```
6. Check the Plastron STOMP Daemon logs to find the URI of the newly 
   imported it. Confirm that it exists.
7. Export the URI value in bash:
   ```bash
   export URI={value copied from log}
   ```
8. Send the update command:
   ```bash
   pytest test_update.py
   ```
9. Confirm that the previously imported item now also has a `dcterms:title`
   value of "Moonpig"
10. Send the publish command:
    ```bash
    pytest test_publish_it.py
    ```
11. Confirm that the previously imported item now has an `rdf:type` of
    `umdaccess:Published`
12. Send the unpublish command:
    ```bash
    pytest test_unpublish_it.py
    ```
13. Confirm that the previously imported item no longer has an `rdf:type` of
    `umdaccess:Published`
14. Send the export command:
    ```bash
    pytest test_export.py
    ```
15. Verify that there is an exported _test-export.tar.gz_
    file in the current directory. List the contents using
    `tar tzf test-export.tar.gz` and you should see:
    ```
    test-export.tar/bagit.txt
    test-export.tar/bag-info.txt
    test-export.tar/manifest-sha512.txt
    test-export.tar/tagmanifest-sha256.txt
    test-export.tar/tagmanifest-sha512.txt
    test-export.tar/manifest-sha256.txt
    test-export.tar/data/Item_metadata.csv
    test-export.tar/data/{SOME UUID}/labor-087802-0001.tif
    ```

[umd-handle]: https://github.com/umd-lib/umd-handle
