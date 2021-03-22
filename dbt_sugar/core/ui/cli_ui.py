"""User Input Collector API."""

import copy
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union, cast

import questionary
from pydantic import BaseModel, validator

DESCRIPTION_PROMPT_MESSAGE = "Please write down your description:"

# TODO: We'll have to us a "plugin"/register design and possibly source this from the dbt project.
# !See issue #65 for most up to date thinking on this.
AVAILABLE_TESTS = ["unique", "not_null"]


class ConfirmQuestion(BaseModel):
    """Validation model for a question confirmation type payload."""

    type: str
    message: str
    name: str
    qmark: str = "?"
    auto_enter: bool = True

    @validator("type")
    def check_type_is_confirm(cls, value):
        assert value == "confirm", "'type' must be set to 'confirm' for this question type."
        return value


class ConfirmModelDoc(ConfirmQuestion):
    """Validatiorn model for a model documentation specific confirm question.

    Extends ConfirmQuestion to add stricter validation on the name field.
    """

    @validator("name")
    def check_name_is_wants_to_document(cls, value):
        assert (
            value == "wants_to_document_model"
        ), "'name' must be set to 'wants_to_document_model'."
        return value


class FreeTextInput(BaseModel):
    """Validation model for a free text input question payload."""

    type: str
    name: str
    message: str

    @validator("type")
    def check_type_is_text(cls, value):
        assert value == "text", "'type' must be set to 'text' for this question type."
        return value


class DescriptionTextInput(FreeTextInput):
    """Validation model for a model or column description text question payload.

    Extends FreeTextInput to add a stricter message validation.
    """

    message: str = DESCRIPTION_PROMPT_MESSAGE

    @validator("message")
    def check_message_text(cls, value):
        assert (
            value == DESCRIPTION_PROMPT_MESSAGE
        ), "Overriding question prompt is not allowed for a description question"
        return value


class MultipleChoiceInput(BaseModel):
    """Validation model for a multiple choice question payload."""

    type: str
    choices: List[str]
    name: str

    @validator("type")
    def check_type_is_choice(cls, value):
        assert value == "checkbox", "'type' must be set to checkbox."
        return value

    @validator("name")
    def check_name_is_cols_to_document(cls, value):
        assert value == "cols_to_document", "'name' must be set to 'cols_to_document'."
        return value


class MultipleChoiceInputWithDict(MultipleChoiceInput):
    """Validation model for a multiple choice question type where the choices list is a dict."""

    choices: Dict[str, str]  # type: ignore # because mypy doesn't like this but I'm ok with it.


class UserInputCollector:
    """User input collector class.

    Validates and orchestrates the interface between the dbt-sugar back end and the questionary API.
    Uses questionary API for CLI collection and imposes some stronger validation rules or flows.

    MAINTENANCE: When we add a lot more question types, we'll likely want to refactor this class to
    use a factory as the view code is going to get long and full of ifs. For now this should get us
    started in the right direction.
    """

    def __init__(
        self,
        question_type: str,
        question_payload: Sequence[Mapping[str, Any]],
        ask_for_tests: bool = True,
        ask_for_tags: bool = True,
        is_paginated: bool = False,
        is_first_page: Optional[bool] = None,
    ) -> None:
        """Constructor for UserInpurCollector.

        Expects a question type and a question payload. The payload must be a list of dictionaries,
        following the `questionary.prompt()` API.

        Args:
            question_type (str): name of the questioning flow type.
                Currently supported: ['model', 'undocumented_cols'].
            question_payload (List[Mapping[str, Any]]): List of dicts following the
                `questionary.prompt()` API requirements (examples below):
                    https://questionary.readthedocs.io/en/stable/pages/advanced.html#question-dictionaries

        Question Payload Examples:
        ```python
        model_doc_payload = [
            {
                "type": "confirm",
                "name": "wants_to_document_model",
                "message": "Model Description: This is my previously documented model. Document?",
                "default": True,
            },
            {
                "type": "text",
                "name": "model_description",
                "message": "Please write down your description:"
            },
        ]

        undocumented_columns_payload = [
            {
                "type": "checkbox",
                "name": "cols_to_document",
                "choices": ["col_a", "col_b"],
                "message": "Select the columns you want to document.",
            }
        ]
        ```
        """
        self._question_type = question_type
        self._question_payload = question_payload
        self._is_valid_question_payload = False
        self._ask_for_tests = ask_for_tests
        self._ask_for_tags = ask_for_tags
        if is_paginated:
            assert isinstance(
                is_first_page, bool
            ), "When using a paginated flow is_first_page cannot be None"
        self._is_paginated = is_paginated
        self._is_first_page = is_first_page

    def _validate_question_payload(self) -> None:
        assert isinstance(self._question_payload, list), "Question payload must be a list of dicts."
        if self._question_type == "model":
            assert (
                len(self._question_payload) == 2
            ), "Payload for model question must contain two dicts."
            for payload_element_index, payload_element in enumerate(self._question_payload):
                if payload_element_index == 0:
                    ConfirmModelDoc(**payload_element)
                if payload_element_index == 1:
                    DescriptionTextInput(**payload_element)

        elif self._question_type == "undocumented_columns":
            MultipleChoiceInput(**self._question_payload[0])

        elif self._question_type == "documented_columns":
            MultipleChoiceInputWithDict(**self._question_payload[0])

        else:
            raise NotImplementedError(f"{self._question_type} is not implemented.")

        self._is_valid_question_payload = True

    def _iterate_through_columns(
        self, cols: List[str]
    ) -> Mapping[str, Mapping[str, Union[str, List[str]]]]:
        """Iterates through a provided list of columns collects documentation info.

        More specifically it asks for the following
        - a column documentation
        - whether user wants to add tests
            - and if so which tests to add from a list of pre-defined/aggreed tests.

        Args:
            cols (List[str]): List of columns to collect description and tests info from.
            ask_for_tests (bool, optional): When set to true UI will ask for whether the user wants
            to add tests for each of the columns. Defaults to True.

        Returns:
            Mapping[str, Mapping[str, Union[str, List[str]]]]: API paylad to be passed to back-end
            that looks like this:
            ```python
            {
                'col_a': {
                    'description': 'Description for col a',
                    'tests': ['unique'],
                    'tags': ['PII']
                    },
                'col_b': {'description': 'Description for col b'}
            }
            ```
        """
        results = dict()
        for column in cols:
            description = questionary.text(
                message=f"Column: '{column}': {DESCRIPTION_PROMPT_MESSAGE}"
            ).unsafe_ask()

            if description:
                results.update({column: {"description": description}})
            else:
                # we still want to make the column end up in the dict but we don't put a desc if
                # empty... not sure about this
                results.update({column: {}})

            # kick in the test flows
            if self._ask_for_tests:
                wants_to_add_tests = questionary.confirm(
                    message="Would you like to add any tests?"
                ).unsafe_ask()
                if wants_to_add_tests:
                    tests = questionary.checkbox(
                        message="Please select one or more tests from the list below",
                        choices=AVAILABLE_TESTS,
                    ).unsafe_ask()
                    if tests:
                        results[column]["tests"] = tests

            # kick in the tags flow
            if self._ask_for_tags:
                wants_to_add_tags = questionary.confirm(
                    message="Would you like to add any tags?"
                ).unsafe_ask()
                if wants_to_add_tags:
                    tags = questionary.text(
                        message="Provide a comma-separated list of tags"
                    ).unsafe_ask()
                    if tags:
                        tags = self.__split_comma_separated_str(tags)
                        results[column]["tags"] = tags
            # remove the column if no info has been given (no tests, and no description).
            if not results[column]:
                _ = results.pop(column)
        return results

    @staticmethod
    def __split_comma_separated_str(tags: str) -> List[str]:
        _tags = []
        if isinstance(tags, str):
            _tags = tags.split(",")
            _tags = [s.strip() for s in _tags]
            return _tags
        raise TypeError(f"Tags can only be strings. You provided a {type(tags)}")

    @staticmethod
    def _document_model(
        question_payload: Sequence[Mapping[str, Any]]
    ) -> Dict[str, Union[bool, str]]:
        results = dict()
        for i, payload_element in enumerate(question_payload):
            results.update(questionary.prompt(payload_element))
            collect_model_description = i < 1 and results.get("wants_to_document_model") is True

            if not results.get("model_description"):
                # we return an empty dict if user decided to not enter a description in the end.
                results = dict()
            if collect_model_description is False:
                # if the user doesnt want to document the model we exit early even if the payload
                # has a second entry which would trigger the description collection
                break
        return results

    def _set_quantifier_word(self) -> str:
        if self._is_first_page and self._is_paginated:
            return "first"
        if not self._is_paginated:
            return "model's"
        return "next"

    def _document_undocumented_cols(
        self,
        question_payload: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Mapping[str, Union[str, List[str]]]]:

        results: Mapping[str, Mapping[str, Union[str, List[str]]]] = dict()
        columns_to_document = question_payload[0].get("choices", list())
        quantifier_word = self._set_quantifier_word()
        # check if user wants to document all columns
        document_all_cols = questionary.confirm(
            message=(
                f"Do you want to document all of the {quantifier_word} {len(columns_to_document)} "
                "undocumented columns?"
            ),
            auto_enter=True,
        ).unsafe_ask()

        if document_all_cols:
            results = self._iterate_through_columns(cols=columns_to_document)
        else:
            # get the list of columns from user
            columns_to_document = questionary.prompt(question_payload)
            results = self._iterate_through_columns(
                cols=columns_to_document["cols_to_document"],
            )
        return results

    def _document_already_documented_cols(
        self,
        question_payload: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Mapping[str, Union[str, List[str]]]]:
        mutable_payload = copy.deepcopy(question_payload)
        mutable_payload = cast(Sequence[Dict[str, Any]], mutable_payload)

        # massage the question payload
        choices = []
        for col, desc in mutable_payload[0].get("choices", dict()).items():
            choices.append(f"{col} | {desc}")
        mutable_payload[0].update({"choices": choices})

        # ask user if they want to see any of the documented columns?
        results = dict()
        document_any_columns = questionary.confirm(
            message="Do you want to document any of the already documented columns in this model?",
            auto_enter=True,
        ).unsafe_ask()

        if document_any_columns:
            columns_to_document = questionary.prompt(mutable_payload)
            _results = self._iterate_through_columns(
                cols=columns_to_document["cols_to_document"],
            )

            # remove description from col key
            for col, desc in _results.items():
                stripped_col_name = col.split("|")[0].strip()
                results.update({stripped_col_name: desc})
            return results

        return results

    def collect(self) -> Mapping[str, Any]:
        """Question orchestractor function.

        Depending on the question type provided on the class will call payload validation and
        orchestrate documentation input collection.

        - When `question_type` == 'model' the following dict is returned:
        ```python
        {'wants_to_document_model': True, 'model_description': 'New def'}
        ```
        - When `question_type` == 'undocumented_columns` or `documented_columns` the following dict
        is returned:
        ```python
        {
            'col_a': {
                'description': 'Description for col a',
                'tests': ['unique']
                },
            'col_b': {'description': 'Description for col b'}
        }
        ```

        Raises:
            NotImplementedError: When a non supported `question_type` is provided

        Returns:
            Mapping[str, Union[bool, str]]: Response object for the backend documentation task.
                See above for examples.
        """
        self._validate_question_payload()

        # Model Documentation Flow
        if self._question_type == "model":
            return self._document_model(self._question_payload)

        # Undocumented Columns Documentation Flow
        if self._question_type == "undocumented_columns":
            return self._document_undocumented_cols(self._question_payload)

        if self._question_type == "documented_columns":
            return self._document_already_documented_cols(self._question_payload)

        raise NotImplementedError(f"{self._question_type} is not implemented.")
