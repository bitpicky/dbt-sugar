"""User Input Collector API."""

from typing import Any, Dict, List, Mapping, Union

import questionary
from pydantic import BaseModel, validator

DESCRIPTION_PROMPT_MESSAGE = "Please write down your description:"


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


class UserInputCollector:
    """User input collector class.

    Validates and orchestrates the interface between the dbt-sugar back end and the questionary API.
    Uses questionary API for CLI collection and imposes some stronger validation rules or flows.

    MAINTENANCE: When we add a lot more question types, we'll likely want to refactor this class to
    use a factory as the view code is going to get long and full of ifs. For now this should get us
    started in the right direction.
    """

    def __init__(self, question_type: str, question_payload: List[Mapping[str, Any]]) -> None:
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

        else:
            raise NotImplementedError(f"{self._question_type} is not implemented.")

        self._is_valid_question_payload = True

    @staticmethod
    def _iterate_through_columns(cols: List[str]) -> Dict[str, str]:
        results = dict()
        for column in cols:
            results.update(
                {column: questionary.text(message=f"{column}: {DESCRIPTION_PROMPT_MESSAGE}").ask()}
            )

        return results

    def collect(self) -> Mapping[str, Union[bool, str]]:
        """Question orchestractor function.

        Depending on the question type provided on the class will call payload validation and
        orchestrate documentation input collection.

        - When `question_type` == 'model' the following dict is returned:
        ```python
        {'wants_to_document_model': True, 'model_description': 'New def'}
        ```
        - When `question_type` == 'undocumented_columns` the following dict is returned:
        ```python
        {'col_a': 'Description for col a', 'col_b': 'Description for col b'}
        ```

        Raises:
            NotImplementedError: When a non supported `question_type` is provided

        Returns:
            Mapping[str, Union[bool, str]]: Response object for the backend documentation task.
                See above for examples.
        """
        results = dict()
        self._validate_question_payload()

        # Model Documentation Flow
        if self._question_type == "model":
            for i, payload_element in enumerate(self._question_payload):
                results.update(questionary.prompt(payload_element))
                if i < 1 and results.get("wants_to_document_model") is False:
                    break
            return results

        # Undocumented Columns Documentation Flow
        if self._question_type == "undocumented_columns":
            # first ask if all columns should be documented,
            columns_to_document = self._question_payload[0].get("choices", list())
            document_all_cols = questionary.confirm(
                message=(
                    f"There are {len(columns_to_document)} undocumented columns. "
                    "Do you want to document them all?"
                ),
                auto_enter=True,
            ).ask()

            # iterate through all cols one by one
            if document_all_cols:
                results = self._iterate_through_columns(cols=columns_to_document)
            else:
                # reduce the list of columns
                columns_to_document = questionary.prompt(self._question_payload)
                # iterate through reduced list
                results = self._iterate_through_columns(
                    cols=columns_to_document["cols_to_document"]
                )
            return results

        raise NotImplementedError(f"{self._question_type} is not implemented.")
