# Job creation

`Job`s represent discrete units of work and are typically focused around a single type of data and group of tables.

First, `Job`s start out from `JobFactory` instances, which take information about a tenant and provide a list of `JobBuilder`s, one for each possible `Job`. Most factories will just define one `Job`, however, sometimes it makes sense to group multiple together (for example, all table truncation jobs could all be spawned from one factory, even though they act on unrelated data and unrelated schemas).

`JobBuilder`s contain information about a `Job` and a list of configuration values; these configuration values will allow the user to turn various fields/tables on and off. Once a user has configured it to their liking, the `Job` is built and `execute` is called, executing all of its stages and parts.

Before you go any further, it is recommended you [read the documentation on stages and parts](./Job%20stages%20and%20parts.md) and review the [interaction flow](./Job%20architecture.png).

## Creating a factory

To create a factory, make a new class in the `jobs` folder which implements `JobFactory` and is declared as a Spring `@Component`. `IPAddressAnonymization` and `TableTruncation` are good examples.

Then, implement `public List<JobBuilder> getBuilders(TenantExecutionContext tenant)`. This should return a list of `JobBuilder`s based on tenant information. For example, we could use the following to create a `Job` which takes two parameters and runs one `JobPart`:

```java
@Override
public List<JobBuilder> getBuilders(TenantExecutionContext tenant) {
  return List.of(
    new JobBuilder(
      "Replace some data",
      "Long description about what this does and why it does it etc etc",
      tenant,
      context,
      List.of(
        new JobConfigurationProperty("enable_replacement"),
        new JobConfigurationProperty("enable_something_else")
      ),
      ctx -> new Job(ctx, List.of("execute")) // only 1 stage called "execute"
        .scheduleParts(
          "execute",
          // settings are available here to use in this lambda to choose parts to create
          List.of(new SomeJobPart(ctx.settings()))
        )
    )
  );
}
```

## Creating a `JobPart`

Most `JobPart`s follow similar patterns and should be built with modularity in mind.

To create a `JobPart`, extend the class and provide a `label` (note: all labels **MUST** be unique within a job!)

Here's a sample `JobPart` that runs a simple `UPDATE` query:

```java
public class SimpleUpdateReplacement extends JobPart {

  private final FieldReference field;
  private final String replacement;

  public SimpleUpdateReplacement(String label, FieldReference field, String replacement) {
    super(label + " (" + field.toString() + ")");
    this.field = field;
    this.replacement = replacement;
  }

  @Override
  protected void execute() {
    this.create()
      .update(field.table(this.tenant()))
      .set(field.column(this.tenant()), field.jsonbSet(this.tenant(), replacement))
      .execute();
  }
}
```
