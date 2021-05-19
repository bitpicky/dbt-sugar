## dbt-sugar [v0.1.0a5] - 2021-05-19

### Under The Hood/Misc

- [#268](https://github.com/bitpicky/dbt-sugar/issues/268) The test adding prompt is a bit more user-friendly and clear when it comes to asking about whether users want to add complex or simple builtin tests.

- [#275](https://github.com/bitpicky/dbt-sugar/issues/275) The dbt password is now obfuscated from the log messages via `logredactor`.

## dbt-sugar [v0.1.0-a.4] - 2021-05-13

### Bug Fixes

- [#263](https://github.com/bitpicky/dbt-sugar/issues/263) SQLAlchemy-redshift is incompatible with versions of SQLAlchemy > 1.4 at the moment which broke the redshift adaptor we shipped in the previous alpha. We're going to pin `sqlalchemy` for the time being and watch any potential progress on the sqlalchemy-redshift pluging [issue](https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/issues/214).

  Thanks to [@ldbrandi](https://github.com/ldbrandi) for reporting [the issue](https://github.com/bitpicky/dbt-sugar/issues/262) and helping with testing.

## dbt-sugar [0.1.0a3] - 2021-05-07

### Bug Fixes

- [#252](https://github.com/bitpicky/dbt-sugar/issues/252) Fixes a bug where the `audit` task was initialised before the `bootstrap` task in a `--bootstrap` run making the audit unable to report on newly bootstrapped model descriptions and still give an inaccurate coverage result.

- [#257](https://github.com/bitpicky/dbt-sugar/issues/257) Fixes a weird bug in the `bootstrap` task which resulted in unpredictable file ordering and failed only in GitHub Actions. We now sort the files before building the dbt model info dict for the bootstrap task.

### Features

- [#256](https://github.com/bitpicky/dbt-sugar/issues/256) Add support for `DBT_PROFILES_DIR` environment variable. If the variable is undefined, dbt profiles directory will default to `~/.dbt`.

## dbt-sugar [v0.1.0a1] - 2021-05-05

### Features

- [#203](https://github.com/bitpicky/dbt-sugar/issues/203) dbt-sugar will now automatically add a `unique` and `not_null` test to the column listed as `primary_key` in your model's `{{ config() }}` block.

- [#212](https://github.com/bitpicky/dbt-sugar/issues/212) Users of snowflake can decide to make the `dbt-sugar` connector use `describe table` when using the `doc` task instead of the native `snowflake.sqlalchemy.get_columns()` by setting the `use_describe_snowflake:` config argument to `true` or by passing the `--use-describe-snowflake` CLI argument. This method is a much faster way to get column information from snowflake's information schema. However, the developers of `snowflake.sqlachemy` opted for the less performant options for reasons that are not entirely clear at this point hence why we're making this an optional and **experimental** feature until we get more clarity in the following [issue](https://github.com/snowflakedb/snowflake-sqlalchemy/issues/221).

- [#229](https://github.com/bitpicky/dbt-sugar/issues/229) dbt-sugar can now connect to Redshift databases by parsing credentials from dbt `profiles.yml`

- [#233](https://github.com/bitpicky/dbt-sugar/issues/233) dbt-sugar can now automatically generate bootstrap model descriptor files (schema.yml) for all your models as well as their columns. Bootstrap model descriptors will contain either placeholders for undocumented columns and model descrptions, unless columns are documented in other models and in that case `bootstrap` will populate those columns with their definitions.

  You can generate bootstraps by calling `dbt-sugar bootstrap`. Running `bootstrap` is particularly useful when you want to run an **exhaustive** `audit` on all your model since the `audit` task does not, by itself, check your models agains the database to make it less resource hungry. A follow up code-change will introduce an `--exhaustive` option on the `audit` task which will call `bootstrap` first and run `audit` after.

- [#238](https://github.com/bitpicky/dbt-sugar/issues/238) dbt-sugar can now add your custom tests via the console, you only need to write the test as you want to look in the schema.yml. dbt-sugar will check if the test PASSES and if it does will add the custom test to your schema.yml.

- [#239](https://github.com/bitpicky/dbt-sugar/issues/239) When running `dbt-sugar audit` users can now trigger all models (as well as their columns) to have at least placeholders in schema.yml files by passing the `--bootstrap` optional argument. This ensures that the audit task is fully aware of all the models before running its checks.

  **NOTE:** the `--bootstrap` option will check for all of your models against your database and may be slower but it will give you the most accurate coverage statistics.

## dbt-sugar [0.0.0] - 2021-04-18

No significant changes.

# dbt-sugar v0.0.0-rc.2 (2021-04-10)

## Bug Fixes

- [\#205](https://github.com/bitpicky/dbt-sugar/issues/205): The snowflake_connector now passes the expected warehouse parameter when creating a connection. This fixes an issue reported by [@sphinks](https://github.com/sphinks).

# dbt-sugar v.0.0.0-rc.0 (2021-04-06)

## Bug Fixes

- [\#187](https://github.com/bitpicky/dbt-sugar/issues/187): The folder `dbt_modules` is now excluded from dbt-sugar's search path
- [\#189](https://github.com/bitpicky/dbt-sugar/issues/189): dbt-sugar now uses the user-provided list to exclude any number of tables from its scope.

  - If a user asks to document a model that is part of the exclude list the app will raise a `ValueError` and tell users why
  - Any model excluded from dbt-sugar's scope will also not be included in any of the `dbt-sugar audit` coverage statistics.

  🚧 **BREAKING-CHANGE**: `excluded_tables` has been renamed to `excluded_models` to be consistent with dbt terminology. Users are advised to update their `sugar_config.yml` if they already used the `excluded_tables` config variable.

## Features

- [\#188](https://github.com/bitpicky/dbt-sugar/issues/188): Users can now exclude folders from the search scope of `dbt-sugar` by providing a list of folder names to exclude in the `sugar_config.yml` via the `excluded_folders:` config argument. `dbt-sugar` will not look for models contained in any of these folders for both the `audit` and `doc` tasks (as well as all other new tasks in this tool unless explicitly mentioned in future releaeses).
- [\#193](https://github.com/bitpicky/dbt-sugar/issues/193): dbt sugar now supports arbitrarily named model property files (schema.yml). This means just like in [dbt core](https://docs.getdbt.com/reference/model-properties) , users can name their model property files any way they like.

# dbt-sugar 0.0.0-b.0 (2021-03-31)

## Bug Fixes

- [\#167](https://github.com/bitpicky/dbt-sugar/issues/167): When creating or modifying a model entry in schema.ymls we now ensure and guarantee that the model `name` and `description` keys are always first. Column names and descriptions are sorted alphabetically by column name however to make the `schema.yml` easier on th eyes.
- [\#173](https://github.com/bitpicky/dbt-sugar/issues/173): Columns part of the yaml is now explicitly ordered to ensure the following:
  - name and descriptions come first even inside of the columns list
  - models names and description also always comes first
  - models and columns are sorted alphabetically across the schema.yml
- [\#180](https://github.com/bitpicky/dbt-sugar/issues/180): Fixes a bug where `dbt-sugar` was looking for the `profiles.yml` file in the current working directory. This was because we were defaulting the `profiles_dir` to `Path(str())` which makes `pathlib` resolve to the current working directory. Resolves [\#177](https://github.com/bitpicky/dbt-sugar/issues/177)

## Features

- [\#101](https://github.com/bitpicky/dbt-sugar/issues/101): Users can control whether dbt-sugar should ask to add tests or tags via the following CLI arguments:
  - via CLI arguments on each run: `--ask-for-tests/--no-ask-for-tests` and `--ask-for-tags/--no-ask-for-tags` or
  - globally per `syrup` in the `sugar_config.yml` via the following arguments: `always_enforce_tests` and `always_add_tags`
- [\#102](https://github.com/bitpicky/dbt-sugar/issues/102): Users can get the documentation and tests statistics with the command `dbt-sugar audit`
  - They can audit a full dbt project, which will return test and documentation coverage statistics for the entire project.
  - They can audit a specific model adding the parameter model, which will give detailed coverage for that specific model.
- [\#31](https://github.com/bitpicky/dbt-sugar/issues/31): `dbt-sugar` requires a `sugar_config.yml` file which makes the following information available at run time:
  - `sugar_canes`: a (list of) dicts called `dbt_projects` which points to a user's dbt project name and path of where to find those projects. An `exclude_tables` list can optionally be provided if users want to exclude some tables from `dbt-sugar`'s analysis scope.
  - `defaults`: a dict of pointing to
    - a default `sugar_cane` to run (if not provided, users need to pass the following CLI arguments `--sugar-cane <cane_name>`
    - a default `target` pointing to a target from the `~/.dbt/profiles.yml` file in order to know which db to talk to and have access to its credentials and object destinations.
- [\#32](https://github.com/bitpicky/dbt-sugar/issues/32): `dbt-sugar` checks that the dbt projects requires by `sugar_canes` are indeed present on disk. If any of them isn't a `MissingDbtProjects` error is raised and user is provided with a list of missing dbt projects. Later on, we might want to relax this to a warning and allow users to choose between a `--strict` or normal run, with normal runs throwing only a warning in this case.
- [\#36](https://github.com/bitpicky/dbt-sugar/issues/36): `dbt-sugar` Doc task that allows the users to Document a model, this task will:
  - Check that the model exists in the DBT repository and where it is (to create the documentation in the same path).
  - Take the model columns from the database.
  - Create/Update the model in the schema.yml with the columns that we have taken in the previous step and the descriptions from the others columns documented (if is a new column name the description will be "No description for this column").
- [\#39](https://github.com/bitpicky/dbt-sugar/issues/39): The concept of `sugar_cane` is renamed to `syrup` to avoid historically loaded interpretations.
- [\#46](https://github.com/bitpicky/dbt-sugar/issues/46): `dbt-sugar` now offers the following UI/Front-End Flow:
  - model-level description writing
  - column-level description witing (only undocumented columns for now)
    - user can choose to document **all undocumented columns in one go** or **select a subset of columns to document**
- [\#50](https://github.com/bitpicky/dbt-sugar/issues/50): `dbt-sugar` now offers documentation UI flow for **already documented columns**. The user is prompted to answer a "yes/no" to whether they want to document already documented columns. If they choose to do so they are presented with a list of columns to choose from. Each column also shows the current description. \<INSERT GIF OF FLOW WHEN GENERATING CHANGELOG\>
- [\#86](https://github.com/bitpicky/dbt-sugar/issues/86): dbt-sugar will now ask users (and allow them) to add tags to their schema.yml files. PR for the backend flow [\#85](https://github.com/bitpicky/dbt-sugar/pull/85).
- [\#93](https://github.com/bitpicky/dbt-sugar/issues/93): Introduces a temporary (until next feature release --or maybe in a patch) regression which will restrict dbt-sugar to only be able to manage **one** dbt project instead of more as was originally intended and hinted at by the use of plural `dbt_projects:` entry in `sugar_config.yml`.

## Under The Hood/Misc

- [\#27](https://github.com/bitpicky/dbt-sugar/issues/27): `dbt-sugar` parses the profile information directly from dbt's `~/.dbt/profiles.yml` it will need to know which dbt project we want to document as well as the target from which to pull database credentials and schemas.
- [\#42](https://github.com/bitpicky/dbt-sugar/issues/42): `dbt-sugar` attempts to find it's config (`sugar_config.yml`) starting from the current working directory from which it is called and up to 4 directories above. If no configuration file is found `dbt-sugar` will throw a `FileNotFoundError` telling the user that the configuration file cannot be found and will print the current working directory to help the user debug the situation.
