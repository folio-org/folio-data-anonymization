# Job stages and parts

`Job`s are divided into `stage`s and `JobPart`s.

## Stages

A `stage` represents a discrete part of a `Job`'s execution and has the following properties:

- Each stage is executed sequentially. A later stage cannot begin until the previous one has fully completed successfully.
- Stages must be pre-defined for the `Job`.
- `JobPart`s within one stage can add additional `JobPart`s for _later_ stages.

Most simple `Job`s will only require one stage. More complex jobs may have multiple stages; for example, username replacement may involve the following (assuming that a temporary table will be created):

- `prepare`ing a temporary table for storing all encountered usernames
- `enumerate`ing every username in the database and storing it in the created table
  - Ideally contains a `JobPart` for each individual table to scan
- `generate`ing replacement usernames to correspond to each original one (likely one `JobPart`)
- `replace`ing the usernames with the generated ones
  - Ideally contains a `JobPart` for each individual table to update
- `cleanup` to destroy the temporary table

## Parts

Parts are the lowest level of effort — **the more granularity the better**! The user will be able to see each part as it runs and, if one fails, restart it. These will also be ran in parallel, therefore, smaller parts will also result in better performance.

These typically should be kept to a single SQL query, for simplicity. See `PartReplaceJSONBWithSQL` for an example.

If there are very large tables where we need to break updates into chunks, it is recommended to use separate stages to 1. quickly `scan` the table and break it into chunks and then 2. `update` each chunk separately.
