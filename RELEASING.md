# Releasing & Packaging

## Preparing the release

- Make sure all code is merged
- On the repo root call `towncrier` to generate the changelog. Good idea to do the first run as a `--draft` to make sure it all looks good before causing a code change.

```bash
towncrier --version <inert_version_name> --draft
```

- `cd changelog/`, news fragments that are left should be deleted.
- copy paste the generated changelog into the **official** document [`CHANGELOG.rst`](CHANGELOG.rst)
- I usually reformat it, add **external contributors** using `@username` as it's not easy to do so automatically so that GitHub automatically links those users.
- bump the version using `bumpversion`. Generally a good idea to run `--dry-run` first to make sure nothing funny happens. I've had surprises with bumpversion doing weird things before.

```bash
bumpversion --tag <release_type> --new-version <semver_number> --verbose --allow-dirty --dry-run
```

- If all looks good we can push the bumpversion commit **and** the tags with: `git push --follow-tags`.

## Releasing the package on PyPI

We use `poetry` to manage packaging so we will use it mostly to also build and send the package over to PyPI.

- `poetry build` will generate the distribution files and wheels
- Push to **test** PyPI. If the test PyPI repo isn't configured the following should do the trick:

```bash
poetry config repositories.testpypi https://test.pypi.org/legacy/
```

- `poetry publish -r testpypi` this will send it over to the test PyPI repo.
- build a new virtual env (to simulate installation for a new user)
- Run installation with pip (making sure we also add an alternate index-url since some packages are not on the test PyPI and this causes dependency installations to fail).

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ dbt-sugar
```

- If the installation goes well, then we should be good but it may be a good idea to run an interactive tests to make sure it's still fine and dandy. If it's good we can publish to the main PyPI by doing

```bash
poetry publish
```
