# Non-interactive mode

- [Invocation](#invocation)
- [Configuration](#configuration)
  - [Global configuration parameters](#global-configuration-parameters)
    - [Example](#example)
  - [Tenants](#tenants)
    - [Example](#example-1)
  - [Jobs](#jobs)
    - [Disabling a job](#disabling-a-job)
    - [Enabling a job with all options](#enabling-a-job-with-all-options)
    - [Enabling a job with specific options](#enabling-a-job-with-specific-options)
    - [Enabling a job but skipping initialization or teardown tasks](#enabling-a-job-but-skipping-initialization-or-teardown-tasks)
    - [\[global_configuration only\] Enabling or disabling removal of unknown configuration properties](#global_configuration-only-enabling-or-disabling-removal-of-unknown-configuration-properties)
- [Outputs](#outputs)

## Invocation

To run in non-interactive mode, provide a configuration file as an argument: `java -jar anonymization-tool.jar path/to/config.json5`.

Logs for both modes will be stored in `logs/application.log`.

## Configuration

Configuration files can be in JSON, JSON5, or YAML and have three sections: global configuration parameters, tenants, and job configuration.

For an example, see [config.sample.json5](./config.sample.json5).

### Global configuration parameters

Located under the `parameters` key, these options allow you to change the behavior of the tool:

| Parameter                     | Description                                                                                   | Suggested value   |
| ----------------------------- | --------------------------------------------------------------------------------------------- | ----------------- |
| `allowedRetries`              | The number of times a failed part will retry (for errors that appear ephemeral)               | 30                |
| `threadPoolSize`              | The number of concurrent Java threads to execute                                              | 100               |
| `connectionPoolSize`          | The number of connections in the pool (recommended slightly larger than the thread pool size) | 120               |
| `queryTimeoutDurationSeconds` | The duration in seconds before a query times out                                              | 600 (ten minutes) |

All of these must be provided.

The following errors will cause job parts to be retried based on `allowedRetries`:

- `PessimisticLockingFailureException` (deadlock)
- `DataAccessResourceFailureException` (communication issue)
- `SQLTransientConnectionException` (timeout/connection pool saturation)
- `QueryTimeoutException` (timeout)

#### Example

```yaml
parameters:
  allowedRetries: 30
  threadPoolSize: 100
  connectionPoolSize: 120
  queryTimeoutDurationSeconds: 600
```

### Tenants

Tenants are defined under the `tenants` key as an array of strings. Each string will be treated **as a regular expression** and matched against the tenant IDs in the environment; be sure to escape any special characters as necessary, and we recommend using anchors (`^` and `$`) to match the entire tenant ID.

#### Example

```yaml
tenants:
  - '^diku$'
  - '^fs09.+'
```

### Jobs

Job configurations are provided under the `jobs` key as an object. Each key in this object refers to a job and the value is its configuration.

> [!NOTE]
>
> **All** jobs must have a configuration entry, even if they are disabled. This is to ensure that jobs added in a future version of the tool are not accidentally enabled or disabled.

Each job can be configured one of the following ways:

#### Disabling a job

To disable a job, simply set `enabled: false` and remove all other properties from its configuration.

```yaml
jobs:
  addresses:
    enabled: false
```

#### Enabling a job with all options

To enable a job with all options, set `enabled: true` and remove all other properties from its configuration.

```yaml
jobs:
  bulk_ops_and_lists:
    enabled: true
```

#### Enabling a job with specific options

For fine-grained control, provide either `whitelistOptions` or `blacklistOptions` (but not both) with regex patterns matching the options you want to enable or disable, respectively. You can find the full list of options in the output of a sample run of the tool.

```yaml
jobs:
  financial_account_numbers:
    enabled: true
    whitelistOptions:
      - '^create-table$'
      - '^drop-table$'
      - '^organizations_storage\\..+' # only affect fields under mod-organizations-storage
  free_text_labels_and_descriptions:
    enabled: true
    blacklistOptions:
      - '^agreements\\.document_attachment\\.da_name$' # exclude this specific field from anonymization
```

#### Enabling a job but skipping initialization or teardown tasks

Some jobs have initialization and teardown tasks (for example, creating a temporary table to hold the map of original values to new values). If you want to run a job but skip these, you can set the convenience options `performSetup` and `performTeardown` (default `true`). This will enable or disable these as applicable, which is useful for resuming a job after a failure.

> [!NOTE]
>
> Jobs which have failing parts will automatically have `performTeardown` effectively disabled, to ensure the job can be resumed in a later run. The created rerun template will have `performSetup` false and `performTeardown` true for the failed job, so that when the rerun template is used, setup tasks will be skipped but teardown tasks will be performed as normal.

```
jobs:
  vendor_names_and_codes:
    enabled: true
    performSetup: false
    performTeardown: false
```

#### [global_configuration only] Enabling or disabling removal of unknown configuration properties

By default, the tool will leave unrecognized properties in `mod-configuration` and `mod-settings` alone, to prevent removal of unknown data. However, the removal of this data can be enabled by setting `removeUnknownModConfigurationEntries` or `removeUnknownModSettingEntries` (default `false`) for the `global_configuration` job.

```yaml
jobs:
  global_configuration:
    enabled: true
    removeUnknownModConfigurationEntries: true
    removeUnknownModSettingEntries: false
```

## Outputs

The tool will generate the following outputs for each tenant:

- `out/{tenantId}-report.json` — A report of the run's results, including which jobs and options were ran, which parts were successful or failed, and a full copy of the configuration for reference. Any errors parts will be accompanied with full tracebacks, to aid in debugging. To verify success, check that `hadFailures` is `false`.
- `out/{tenantId}-rerun.json` (failed executions only) — A new configuration file based on the original configuration but with the following changes to allow for easy resumption of the failed jobs:
  - Only includes this tenant and the jobs that had failures, to allow for a more targeted rerun.
  - Sets `performSetup` to `false` and `performTeardown` to `true` for failed jobs, to skip initialization tasks that were already completed but still perform any necessary teardown tasks.

Examples of a report can be found [here](./sample-report.json) and a rerun configuration can be found [here](./sample-rerun.json).

**If re-running the tool using the rerun configuration is successful, the dataset has been fully anonymized. There is no need to run the full series of anonymization tasks.**
