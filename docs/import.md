# Import Command

## CLI Usage

```
$ plastron import -h
usage: plastron import [-h] [-m MODEL] [-l LIMIT] [--validate-only]
                       [--make-template TEMPLATE_FILE] [--access ACCESS]
                       [--member-of MEMBER_OF]
                       [--binaries-location BINARIES_LOCATION]
                       [--container CONTAINER] [--job-id JOB_ID] [--resume]
                       [import_file]

Import data to the repository

positional arguments:
  import_file           name of the file to import from

optional arguments:
  -h, --help            show this help message and exit
  -m MODEL, --model MODEL
                        data model to use
  -l LIMIT, --limit LIMIT
                        limit the number of rows to read from the import file
  --validate-only       only validate, do not do the actual import
  --make-template TEMPLATE_FILE
                        create a CSV template for the given model
  --access ACCESS       URI or CURIE of the access class to apply to new items
  --member-of MEMBER_OF
                        URI of the object that new items are PCDM members of
  --binaries-location BINARIES_LOCATION
                        where to find binaries; either a path to a directory,
                        a "zip:<path to zipfile>" URI, an SFTP URI in the form
                        "sftp://<user>@<host>/<path to dir>", or a URI in the
                        form "zip+sftp://<user>@<host>/<path to zipfile>"
  --container CONTAINER
                        parent container for new items; defaults to the
                        RELPATH in the repo configuration file
  --job-id JOB_ID       unique identifier for this job; defaults to
                        "import-{timestamp}"
  --resume              resume a job that has been started; requires --job-id
                        {id} to be present
```

## Daemon Usage

STOMP message headers:

```
PlastronCommand: import
PlastronJobId: JOB_ID
PlastronArg-model: MODEL
PlastronArg-limit: LIMIT 
PlastronArg-validate-only: {true|false}
PlastronArg-resume: {true|false}
PlastronArg-access: ACCESS
PlastronArg-member-of: MEMBER_OF
PlastronArg-binaries-location: BINARIES_LOCATION
PlastronArg-container: CONTAINER
```

## Configuration

The following keys are used in the `COMMANDS/IMPORT` section of the config file:

| Name            | Purpose |
|-----------------|---------|
|`JOBS_DIR`       |Base directory for storing [job](#jobs) information. Defaults to `jobs` in the working directory|
|`SSH_PRIVATE_KEY`|Path to the private key to use when retrieving binaries over SFTP|

## Jobs

Every time the import command runs, it is in the context of a _job_. Plastron
stores the configuration specified when running the import, and the source CSV
file, as well as a log of successfully imported items, and logs of items that
were dropped during a particular run.

After starting a job, you can use its job ID to resume it at a later time. When
resuming a job, plastron will check the completed log for the job and skip any
items recorded there.

The `completed.log.csv` has the following columns:

| Name      | Purpose |
|-----------|---------|
|`id`       |Unique identifier for this item (within the context of this job)|
|`timestamp`|Date and time when this item was successfully imported|
|`title`    |Title of the item|
|`uri`      |URI of the item in the target repository|

You may specify a job ID on the command line using the `--job-id` argument. If
you do not provide one, Plastron will generate one using the current timestamp.

If, during a run, an item cannot be loaded for any reason, that item is recorded
to a dropped item log for that run, along with the reason for the failure.

Dropped item logs have the following columns:

| Name      | Purpose |
|-----------|---------|
|`id`       |Unique identifier for this item (within the context of this job)|
|`timestamp`|Date and time when this item failed to import|
|`title`    |Title of the item|
|`uri`      |URI of the item in the target repository; this may be empty if the item is new|
|`reason`   |Short description of the error leading to failure to import|

### Example

Start a new job:

```bash
plastron -c repo.yml import \
    --model Item \
    --binaries-location /path/to/binaries \
    --member-of http://localhost:8080/rest/collections/foo \
    --container /objects \
    --job-id import-foo-1
    metadata.csv
```

Plastron will create the following structure in the `JOBS_DIR`:

```
{JOBS_DIR}
    +- import-foo-1           # job ID
        +- completed.log.csv  # completed item log
        +- config.yml         # command-line options
        +- source.csv         # copy of metadata.csv
```

Resume that job later:

```bash
plastron -c repo.yml import \
    --job-id import-foo-1
    --resume
```

Any dropped items from a particular run will be recorded in
`{JOBS_DIR}/import-foo-1/dropped-{run_timestamp}.csv`.