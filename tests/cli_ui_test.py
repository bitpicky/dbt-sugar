import pytest


@pytest.mark.parametrize(
    "question_type, question_payload, expected_results",
    [
        (
            "model",
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
            {"prompt_ret": {"wants_to_document_model": True, "model_description": "New def"}},
        ),
        (
            "model",
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
            {"prompt_ret": {"wants_to_document_model": False}},
        ),
        (
            "undocumented_columns",
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": ["col_a", "col_b"],
                    "message": "Select the columns you want to document.",
                }
            ],
            {
                "confirm_ret": True,
                "prompt_ret": {"col_a": "Custom desc", "col_b": "Custom desc"},
                "text_ret": "Custom desc",
            },
        ),
        (
            "undocumented_columns",
            [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": ["col_a", "col_b"],
                    "message": "Select the columns you want to document.",
                }
            ],
            {
                "confirm_ret": False,
                "prompt_ret": {"col_a": "Custom desc"},
                "non_full_cols": {"cols_to_document": ["col_a"]},
                "text_ret": "Custom desc",
            },
        ),
        (
            "non_implemented",
            [],
            {},
        ),
    ],
)
def test_collect(mocker, question_type, question_payload, expected_results):
    from dbt_sugar.core.ui.cli_ui import UserInputCollector

    # mock for the questionary.Question --this was a bit of a fuckery to test...
    class Question:
        def __init__(self, return_value):
            self._return_value = return_value

        def ask(self):
            return self._return_value

    if question_type == "model":
        mocker.patch("questionary.prompt", return_value=expected_results.get("prompt_ret"))
    else:
        mocker.patch("questionary.prompt", return_value=expected_results.get("non_full_cols"))
        mocker.patch(
            "questionary.confirm", return_value=Question(expected_results.get("confirm_ret"))
        )
        mocker.patch("questionary.text", return_value=Question(expected_results.get("text_ret")))
    input_collector = UserInputCollector(question_type, question_payload)

    if question_type == "non_implemented":
        with pytest.raises(NotImplementedError):
            results = input_collector.collect()
    else:
        results = input_collector.collect()
        assert results == expected_results.get("prompt_ret")
