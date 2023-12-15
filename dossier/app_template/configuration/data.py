"""Application data configuration."""
import os
import json


class DataConfig:
    """Retrieve information from the configuration files located in ./resources directory."""

    _input_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "resources"
        )
    )
    _infra_filepath = os.path.join(_input_path, "infra.json")

    def __init__(self):
        """Initialize data configuration."""
        self._prepare_infra_configuration()

    def _prepare_infra_configuration(self):
        """Load the infra configuration."""
        with open(self._infra_filepath) as json_file:
            self.infra_config = json.load(json_file)
