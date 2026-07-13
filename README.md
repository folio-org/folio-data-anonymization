# folio-data-anonymization

The FOLIO Data Anonymization tool allows hosting teams to anonymize patron and vendor information in-place in a given FOLIO instance, allowing institutions to retain privacy while allowing their data to be safely shared and used for testing.

For more information, read [this page on which data are in scope](https://folio-org.atlassian.net/wiki/spaces/DQA/pages/1880752142/Anonymization+Rules) and [this page detailing our methodologies](https://folio-org.atlassian.net/wiki/spaces/DQA/pages/1880588321/Anonymization+Methodologies).

## Usage

### Preparing the environment

This tool requires a PostgreSQL database connection to the data to be anonymized. We recommend:

- _NOT_ running this tool while FOLIO modules are running (system tasks/timers can cause competition and potential race conditions),
- Doing a full vacuum of the database before running this tool to ensure accurate table size information and optimal performance,
- Running the tool somewhere close to the database (at least in the cloud in the same AWS region) to ensure a stable connection and reduce latency,
- Increasing the database cluster resources to ensure it can handle **heavy sustained load** (and, if necessary, disabling applicable alarms).

### Running the tool

#### Setup

The tool runs as a command-line application and will need to remain running for the life of the process, which can take many hours depending on the database size and tool's configuration. As a result, we recommend running the tool in a `screen` or `tmux` session to ensure interrupted connections do not stop the process.

To run the tool, first get the JAR either from [the package page](https://github.com/folio-org/folio-data-anonymization/packages/3014659) (or build it yourself with `mvn package`).

#### Invocation

There are two modes for the tool: interactive and non-interactive. The interactive mode allows you to select which tenants and jobs to run dynamically, and monitor progress, while the non-interactive mode allows you to provide a configuration file with all necessary information to run the tool without any further input.

Both modes require the following environment variables for database connection:

For the FOLIO database:

- `DB_USERNAME`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_DATABASE`

For the Keycloak database:

- `KC_USERNAME` (if different from `DB_USERNAME`)
- `KC_PASSWORD` (if different from `DB_PASSWORD`)
- `KC_HOST` (if different from `DB_HOST`)
- `KC_PORT` (if different from `DB_PORT`)
- `KC_DATABASE` (defaults to `keycloak`)

> [!NOTE]
>
> If you do not plan on performing Keycloak anonymization, set `KC_DATABASE` to the same value as `DB_DATABASE` to prevent connection errors on startup.

To run in interactive mode, simply run the JAR with no arguments: `java -jar anonymization-tool.jar`.

To run in non-interactive mode, provide a configuration file as an argument: `java -jar anonymization-tool.jar path/to/config.json5`.

Logs for both modes will be stored in `logs/application.log`.

For more information about these modes, see their documentation pages ([interactive mode](./docs/interactive-mode.md) and [non-interactive mode](./docs/non-interactive-mode.md)).

### After anonymization is performed

Once the tool has run successfully, the database is ready to be used in a FOLIO environment. To ensure complete anonymization of the environment, the following **must** be performed by the hosting team:

- Ensure that all module configurations point to the new, anonymized database (and not the original database),
- Ensure that all module configurations point to a new, empty S3-like storage (and not the original storage), and
- Ensure that any other module configurations that point to an external source are updated as applicable.

In addition to the above mandatory steps, we recommend:

- Performing a full vacuum of the database to ensure optimal performance.

Once everything is ready, modules and Keycloak can be started and the environment can be used.

## Issue tracking

See [DANON](https://issues.folio.org/projects/DANON) in the [FOLIO issue tracker](https://folio-org.atlassian.net/).
