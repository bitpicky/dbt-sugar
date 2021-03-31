dbt-sugar 0.0.0-b.0 (2021-03-31)
================================

Bug Fixes
---------

- `#167 <https://github.com/bastienboutonnet/sheetwork/issues/167>`_: When creating or modifying a model entry in schema.ymls we now ensure and guarantee that the model ``name`` and ``description`` keys are always first. Column names and descriptions are sorted alphabetically by column name however to make the ``schema.yml`` easier on th eyes.


- `#173 <https://github.com/bastienboutonnet/sheetwork/issues/173>`_: Columns part of the yaml is now explicitly ordered to ensure the following:
  - name and descriptions come first even inside of the columns list
  - models names and description also always comes first
  - models and columns are sorted alphabetically across the schema.yml


- `#180 <https://github.com/bastienboutonnet/sheetwork/issues/180>`_: Fixes a bug where ``dbt-sugar`` was looking for the ``profiles.yml`` file in the current working directory. This was because we were defaulting the ``profiles_dir`` to ``Path(str())`` which makes ``pathlib`` resolve to the current working directory. Resolves `#177 <https://github.com/bitpicky/dbt-sugar/issues/177>`_



Features
--------

- `#101 <https://github.com/bastienboutonnet/sheetwork/issues/101>`_: Users can control whether dbt-sugar should ask to add tests or tags  via the following CLI arguments:
  - via CLI arguments on each run:  ``--ask-for-tests/--no-ask-for-tests`` and ``--ask-for-tags/--no-ask-for-tags`` or
  - globally per ``syrup`` in the ``sugar_config.yml`` via the following arguments: ``always_enforce_tests`` and ``always_add_tags``


- `#102 <https://github.com/bastienboutonnet/sheetwork/issues/102>`_: Users can get the documentation and tests statistics with the command ``dbt-sugar audit``
  - They can audit a full dbt project, which will return test and documentation coverage statistics for the entire project.
  - They can audit a specific model adding the parameter model, which will give detailed coverage for that specific model.


- `#31 <https://github.com/bastienboutonnet/sheetwork/issues/31>`_: ``dbt-sugar`` requires a ``sugar_config.yml`` file which makes the following information available at run time:
  - ``sugar_canes``: a (list of) dicts called ``dbt_projects`` which points to a user's dbt project name and path of where to find those projects. An ``exclude_tables`` list can optionally be provided if users want to exclude some tables from ``dbt-sugar``'s analysis scope.
  - ``defaults``: a dict of pointing to
    - a default ``sugar_cane`` to run (if not provided, users need to pass the following CLI arguments ``--sugar-cane <cane_name>``
    - a default ``target`` pointing to a target from the ``~/.dbt/profiles.yml`` file in order to know which db to talk to and have access to its credentials and object destinations.


- `#32 <https://github.com/bastienboutonnet/sheetwork/issues/32>`_: ``dbt-sugar`` checks that the dbt projects requires by ``sugar_canes`` are indeed present on disk. If any of them isn't a ``MissingDbtProjects`` error is raised and user is provided with a list of missing dbt projects. Later on, we might want to relax this to a warning and allow users to choose between a ``--strict`` or normal run, with normal runs throwing only a warning in this case.


- `#36 <https://github.com/bastienboutonnet/sheetwork/issues/36>`_: ``dbt-sugar`` Doc task that allows the users to Document a model, this task will:
  - Check that the model exists in the DBT repository and where it is (to create the documentation in the same path).
  - Take the model columns from the database.
  - Create/Update the model in the schema.yml with the columns that we have taken in the previous step and the descriptions from the others columns documented (if is a new column name the description will be "No description for this column").


- `#39 <https://github.com/bastienboutonnet/sheetwork/issues/39>`_: The concept of ``sugar_cane`` is renamed to ``syrup`` to avoid historically loaded interpretations.


- `#46 <https://github.com/bastienboutonnet/sheetwork/issues/46>`_: ``dbt-sugar`` now offers the following UI/Front-End Flow:
  - model-level description writing
  - column-level description witing (only undocumented columns for now)
    - user can choose to document **all undocumented columns in one go** or **select a subset of columns to document**


- `#50 <https://github.com/bastienboutonnet/sheetwork/issues/50>`_: ``dbt-sugar`` now offers documentation UI flow for **already documented columns**. The user is prompted to answer a "yes/no" to whether they want to document already documented columns. If they choose to do so they are presented with a list of columns to choose from. Each column also shows the current description.
  <INSERT GIF OF FLOW WHEN GENERATING CHANGELOG>


- `#86 <https://github.com/bastienboutonnet/sheetwork/issues/86>`_: dbt-sugar will now ask users (and allow them) to add tags to their schema.yml files. PR for the backend flow `#85 <https://github.com/bitpicky/dbt-sugar/pull/85>`_.


- `#93 <https://github.com/bastienboutonnet/sheetwork/issues/93>`_: Introduces a temporary (until next feature release --or maybe in a patch) regression which will restrict dbt-sugar to only be able to manage **one** dbt project instead of more as was originally intended and hinted at by the use of plural ``dbt_projects:`` entry in ``sugar_config.yml``.



Under The Hood/Misc
-------------------

- `#27 <https://github.com/bastienboutonnet/sheetwork/issues/27>`_: ``dbt-sugar`` parses the profile information directly from dbt's ``~/.dbt/profiles.yml`` it will need to know which dbt project we want to document as well as the target from which to pull database credentials and schemas.


- `#42 <https://github.com/bastienboutonnet/sheetwork/issues/42>`_: ``dbt-sugar`` attempts to find it's config (``sugar_config.yml``) starting from the current working directory from which it is called and up to 4 directories above. If no configuration file is found ``dbt-sugar`` will throw a ``FileNotFoundError`` telling the user that the configuration file cannot be found and will print the current working directory to help the user debug the situation.
