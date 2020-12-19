import pytest


@pytest.mark.parametrize("fake_needs_update", [True, False])
def test_check_and_compare_version(fake_needs_update, mocker):
    from dbt_sugar.core.utils import check_and_compare_version

    # mock the call to pypi
    mocker_result = "0.0.0"
    if fake_needs_update:
        mocker_result = "1.0.0"

    def mock_get_version_pypi():
        return mocker_result

    # monkeypatch.setattr("luddite", "get_version_pypi", mock_get_version_pypi)

    mocker.patch("luddite.get_version_pypi", return_value=mocker_result)
    dummy_version = "0.0.0"
    needs_update, pypi_version = check_and_compare_version(dummy_version)
    if fake_needs_update:
        assert needs_update is True
        assert pypi_version == "1.0.0"
    else:
        assert needs_update is False
        assert pypi_version == "0.0.0"
