# Import Command

## CLI Usage

```
$ plastron import -h
usage: plastron import [-h] [-m MODEL] [-l LIMIT] [-% PERCENTAGE]
                       [--validate-only] [--make-template FILENAME]
                       [--access URI|CURIE] [--member-of URI]
                       [--binaries-location LOCATION] [--container PATH]
                       [--job-id JOB_ID] [--resume]
                       [--extract-text-from MIME_TYPES]
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
  -% PERCENTAGE, --percent PERCENTAGE
                        select an evenly spaced subset of items to import; the
                        size of this set will be as close as possible to the
                        specified percentage of the total items
  --validate-only       only validate, do not do the actual import
  --make-template FILENAME
                        create a CSV template for the given model
  --access URI|CURIE    URI or CURIE of the access class to apply to new items
  --member-of URI       URI of the object that new items are PCDM members of
  --binaries-location LOCATION
                        where to find binaries; either a path to a directory,
                        a "zip:<path to zipfile>" URI, an SFTP URI in the form
                        "sftp://<user>@<host>/<path to dir>", or a URI in the
                        form "zip+sftp://<user>@<host>/<path to zipfile>"
  --container PATH      parent container for new items; defaults to the
                        RELPATH in the repo configuration file
  --job-id JOB_ID       unique identifier for this job; defaults to
                        "import-{timestamp}"
  --resume              resume a job that has been started; requires --job-id
                        {id} to be present
  --extract-text-from MIME_TYPES, -x MIME_TYPES
                        extract text from binaries of the given MIME types,
                        and add as annotations
```

## Daemon Usage

STOMP message headers:

```
PlastronCommand: import
PlastronJobId: JOB_ID
PlastronArg-model: MODEL
PlastronArg-limit: LIMIT
PlastronArg-percent: PERCENTAGE
PlastronArg-validate-only: {true|false}
PlastronArg-resume: {true|false}
PlastronArg-access: ACCESS
PlastronArg-member-of: MEMBER_OF
PlastronArg-binaries-location: BINARIES_LOCATION
PlastronArg-container: CONTAINER
PlastronArg-extract-text: MIME_TYPES
PlastronArg-structure: {flat|hierarchical}
PlastronArg-relpath: PATH
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

### Import Failures

Items that cannot be imported during a run are categorized as either
"invalid" or "failed".

#### Invalid Items

Invalid items are items that fail metadata validation, and are recorded in
the "dropped-invalid" log for that run, along with the reason for the failure.

Invalid items will likely require changes to the source CSV file, or some other
action on the part of the user (such as adding missing files).

#### Failed Items

Failed items are items that could not be imported due to problems adding
records to the repository, and are recorded in the "dropped-failed" log for that
run, along with the reason for the failure.

Some failures may occcur due to transient network issues. In those cases,
resuming the import should allow those items to tbe added.

### Dropped Item Logs

Both the "dropped-invalid" and "dropped-failed" item logs have the following
columns:

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
    --job-id import-foo-1 \
    --resume
```

Any dropped items from a particular run will be recorded in:

* `{JOBS_DIR}/import-foo-1/dropped-failed-{run_timestamp}.csv`
* `{JOBS_DIR}/import-foo-1/dropped-invalid-{run_timestamp}.csv`

## Percentage Imports

You may use the `-%` or `--percent` option to import only a subset of the items
in the import metadata CSV. Repeated use of this option with the same job will
select new subsets of items that have not yet been imported.

For example, start a job that has 50 items total, but only load 10% at first:

```bash
plastron -c repo.yml import \
    --model Item \
    --binaries-location /path/to/binaries \
    --member-of http://localhost:8080/rest/collections/foo \
    --container /objects \
    --job-id percentile-job \
    --percent 10
```

Plastron will only import 5 items (10% of 50), as evenly spaced within the set of
uncompleted items as possible.

If you resume the job with the `--percent 10` option again:

```bash
plastron -c repo.yml import \
    --job-id percentile-job \
    --resume \
    --percent 10
```

Plastron will import 5 more items, selected from the 45 items that were not
imported during the first run of the job.

If you specify a percentage that would generate a subset larger than the number
of remaining items, Plastron will import all the remaining items.
