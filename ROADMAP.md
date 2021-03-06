# dbt-sugar Roadmap 🛣 🚧

## Vision

We created dbt-sugar because documenting dbt models consistently enforcing tests can easily slip out of the developer's workflow. In the regular dbt workflow, you often have to open a few `schema.yml` files, call `dbt test` etc. If you want to homogenise column descriptions across your models, it means you will have to remember or search for and modify them. dbt-sugar takes care of doing all that for you (and more).

We're currently focusing on getting a basic minimum viable product out for dbt-sugar. You can follow the development and planned work by monitoring our [projects page](https://github.com/bitpicky/dbt-sugar/projects).

## MVP Release: v0.0.0 Nils Frahm - All Melody

This release focuses on shipping basic features to get the project going. We want to solve the following:

- Provide users with a tool to **document dbt models and their columns via a simple CLI flow**.
- Provide users with a tool to easily add **tests** (and assert that they are true right at the moment where they are being added) to their dbt models.
- Provide users with a tool to easily add **tags** to their dbt models.

## v0.1.0 - Toxe - Honey Island

This release will focus on making the basic features shipped in `v0.0.0` smarter:

- Auto-generate tests on systematic columns. For example, users often declare `primary_key`s in their dbt `{{config()}}`. We could use this information to auto-add or suggest dbt builtin tests such as `['unique', 'not_null']` or even relational integrity tests.
- Allow users to propose patterns according to their column naming conventions which would make dbt-sugar **auto-add tags** such as **PII** field.
- Allow users to propose patterns and test rules (test name + optional argument) which dbt-sugar could auto add. For example, in a [recent article](https://emilyriederer.netlify.app/post/column-name-contracts/), written by [Emily Riederer](https://emilyriederer.netlify.app/) on the concept of "Column names as contracts", they propose that data modellers should follow strict and almost auto-documenting column names such as `ID_<ENTITY>`, or `CAT_<ENTITY>` to indicate the nature of the column content but, and most importantly, to allow easy machine parsing of those columns at consumption point.

## v0.2.0

In this, yet to be named release, we plan to add a new set of tasks besides documentation:

- An **audit** task which would provide test and documntation coverage to users.
- A **validate** task which would allow users to compare their development table vs. a production version and perform a series of tests such as:
  - column and row counts to be able to easily validate the impact of your changes between development and production environments.
  - distributional analyses (means, mediand, histograms etc) --we will probably leverage the [Great Expectations API](https://greatexpectations.io/) to do so.

## Do you want to contribute to ideas or functionality?

We love it when users engage with us! You can help us build faster or more awesome features by doing any of the following:

- check out our [CONTRIBUTING.md](CONTRIBUTING.md)
- [create a new issue](https://github.com/bitpicky/dbt-sugar/issues/new) to discus a feature or report a bug.
- join our [Discord](https://discord.gg/cQB49ejbCA) to engage with community members and developers.
