import pytest

from dbt_sugar.core.ui.cli_ui import UserInputCollector


class Question:
    def __init__(self, return_value):
        self._return_value = return_value

    def ask(self):
        return self._return_value


@pytest.mark.parametrize(
    "question_payload, questionary_outputs, expected_results",
    [
        pytest.param(
            [
                {
                    "type": "confirm",
                    "name": "wants_to_document_model",
                    "message": "Model Description: This is my previously documented model. Document?",
                    "default": True,
                },
                {
                    "type": "text",
                    "name": "model_description",
                    "message": "Please write down your description:",
                },
            ],
            {"wants_to_document_model": True, "model_description": "New def"},
            {"wants_to_document_model": True, "model_description": "New def"},
            id="yes_document_and_def_given",
        ),
        pytest.param(
            [
                {
                    "type": "confirm",
                    "name": "wants_to_document_model",
                    "message": "Model Description: This is my previously documented model. Document?",
                    "default": True,
                },
                {
                    "type": "text",
                    "name": "model_description",
                    "message": "Please write down your description:",
                },
            ],
            {"wants_to_document_model": False},
            {"wants_to_document_model": False},
            id="no_document_model",
        ),
    ],
)
def test__document_model(mocker, question_payload, questionary_outputs, expected_results):

    mocker.patch("questionary.prompt", return_value=questionary_outputs)
    results = UserInputCollector(question_type="model", question_payload=question_payload).collect()
    if questionary_outputs.get("wants_to_document_model") is False:
        assert results == dict()
    else:
        assert results == expected_results


@pytest.mark.parametrize(
    "question_payload, questionary_outputs, expected_results",
    [
        pytest.param(
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": ["col_a", "col_b"],
                    "message": "Select the columns you want to document.",
                },
            ],
            {
                "confirm_return": True,
                "prompt_return": {"col_a": "Custom desc", "col_b": "Custom desc"},
            },
            {"col_a": {"description": "Custom desc"}, "col_b": {"description": "Custom desc"}},
            id="document_all_undocumented",
        ),
        pytest.param(
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": ["col_a", "col_b"],
                    "message": "Select the columns you want to document.",
                },
            ],
            {
                "confirm_return": False,
                "prompt_return": {"cols_to_document": ["col_a"]},
            },
            {"col_a": {"description": "Custom desc"}},
            id="document_only_some_columns",
        ),
    ],
)
def test__document_undocumented_columns(
    mocker, question_payload, questionary_outputs, expected_results
):
    from dbt_sugar.core.ui.cli_ui import UserInputCollector

    mocker.patch(
        "questionary.confirm", return_value=Question(questionary_outputs["confirm_return"])
    )
    mocker.patch("questionary.prompt", return_value=questionary_outputs["prompt_return"])
    mocker.patch("questionary.text", return_value=Question("Custom desc"))
    results = UserInputCollector(
        question_type="undocumented_columns", question_payload=question_payload
    )._document_undocumented_cols(question_payload=question_payload, ask_for_tests=False)
    assert results == expected_results


@pytest.mark.parametrize(
    "question_payload, questionary_outputs, expected_results",
    [
        pytest.param(
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": {"col_a": "Column a description", "col_b": "Column b description"},
                    "message": "Select the columns you want to document.",
                },
            ],
            {
                "confirm_return": True,
                "prompt_return": {"cols_to_document": ["col_a"]},
            },
            {"col_a": {"description": "Custom desc"}},
            id="document_some_documented_cols",
        ),
        pytest.param(
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": {"col_a": "Column a description", "col_b": "Column b description"},
                    "message": "Select the columns you want to document.",
                },
            ],
            {
                "confirm_return": False,
                "prompt_return": {},
            },
            {},
            id="no_document_already_documented_cols",
        ),
    ],
)
def test__document_already_documented_cols(
    mocker, question_payload, questionary_outputs, expected_results
):
    mocker.patch(
        "questionary.confirm", return_value=Question(questionary_outputs["confirm_return"])
    )
    mocker.patch("questionary.prompt", return_value=questionary_outputs["prompt_return"])
    mocker.patch("questionary.text", return_value=Question("Custom desc"))
    results = UserInputCollector(
        question_type="undocumented_columns", question_payload=question_payload
    )._document_already_documented_cols(question_payload=question_payload, ask_for_tests=False)
    assert results == expected_results


@pytest.mark.parametrize(
    "question_payload, expected_results",
    [
        pytest.param(
            {"col_list": ["column_a", "column_b"], "ask_for_tests": False},
            {
                "column_a": {"description": "Dummy description"},
                "column_b": {"description": "Dummy description"},
            },
            id="document_columns_no_test",
        ),
        pytest.param(
            {"col_list": ["column_a", "column_b"], "ask_for_tests": True},
            {
                "column_a": {"description": "Dummy description", "tests": ["unique"]},
                "column_b": {"description": "Dummy description", "tests": ["unique"]},
            },
            id="document_columns_yes_test",
        ),
    ],
)
def test__iterate_through_columns(mocker, question_payload, expected_results):
    mocker.patch("questionary.text", return_value=Question("Dummy description"))
    mocker.patch("questionary.checkbox", return_value=Question(["unique"]))
    mocker.patch("questionary.confirm", return_value=Question(question_payload["ask_for_tests"]))
    results = UserInputCollector(
        "undocumented_columns", question_payload=[]
    )._iterate_through_columns(
        cols=question_payload["col_list"], ask_for_tests=question_payload["ask_for_tests"]
    )
    assert results == expected_results
