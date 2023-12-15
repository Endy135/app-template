"""Application environment configuration."""
import os
import yaml


class AppConfig:
    """Load application environment configuration, e.g. credentials, logging."""

    _logging_conf_filepath = os.path.join(os.environ["UNXCONF"], "logging.conf.yml")
    _application_filepath = os.path.join(os.environ["UNXCONF"], "application.yml")

    def __init__(self):
        """Initialize application configuration.

        The 'application.yml' file is a symlink to one of the following conf files,
        which all have different credentials:
            - development.yml
            - preproduction.yml

        """
        self._prepare_logging()
        try:
            with open(self._application_filepath) as f:
                self._application_config = yaml.load(f.read(), Loader=yaml.FullLoader)
            self._prepare_brcdb_configuration()
            self._prepare_mail_configuration()
            self._prepare_sftp_configuration()
            self._prepare_azureml_configuration()
        except FileExistsError:
            pass

    @property
    def brcdb_config(self):
        """BRC database configuration."""
        if hasattr(self, "_brcdb_config"):
            return self._brcdb_config
        else:
            raise KeyError(
                f"No brcdb configuration found. Please fill it in {self._logging_conf_filepath}"
            )

    @property
    def mail_config(self):
        """Mail configuration."""
        if hasattr(self, "_mail_config"):
            return self._mail_config
        else:
            raise KeyError(
                f"No mail configuration found. Please fill it in {self._logging_conf_filepath}"
            )

    @property
    def sftp_config(self):
        """SFTP configuration."""
        if hasattr(self, "_sftp_config"):
            return self._sftp_config
        else:
            raise KeyError(
                f"No sftp configuration found. Please fill it in {self._logging_conf_filepath}"
            )

    @property
    def azureml_config(self):
        """Azure Machine Learning configuration."""
        if hasattr(self, "_azureml_config"):
            return self._azureml_config
        else:
            raise KeyError(
                f"No azureml configuration found. Please fill it in {self._logging_conf_filepath}"
            )

    def _prepare_logging(self):
        """Load logging configuration.

        The logging file is a timebased rotating file.

        """
        # Create log directory
        try:
            if not os.path.exists(os.environ["UNXLOG"]):
                os.makedirs(os.environ["UNXLOG"])
        except FileExistsError:
            pass

        # Create logging config
        with open(self._logging_conf_filepath) as f:
            dict_conf = yaml.load(f.read(), Loader=yaml.FullLoader)
            # Add filename
            prefix = os.environ["UNXPACKAGE"].replace("_", "-")
            for handler in dict_conf["handlers"].keys():
                if handler.startswith("file"):
                    filename = f"{prefix}-{handler.split('_')[1]}.log"
                    dict_conf["handlers"][handler][
                        "filename"
                    ] = f"{os.environ['UNXLOG']}/{filename}"

            import logging.config
            logging.config.dictConfig(dict_conf)

    def _prepare_brcdb_configuration(self):
        """Prepare BRC database configuration."""
        if "brcdb" in self._application_config:
            brcdb_config = self._application_config["brcdb"]
            mandatory_params = [
                "host", "short_name", "service_name", "username", "password", "port"
            ]
            self._check_configuration_parameters("brcdb", brcdb_config, mandatory_params)
            self._brcdb_config = brcdb_config

    def _prepare_mail_configuration(self):
        """Prepare mail configuration."""
        if "mail" in self._application_config:
            mail_config = self._application_config["mail"]
            mandatory_params = [
                "host", "sender", "user", "password", "to_recipients", "cc_recipients"
            ]
            self._check_configuration_parameters("mail", mail_config, mandatory_params)
            self._mail_config = mail_config

    def _prepare_sftp_configuration(self):
        """Prepare SFTP configuration."""
        if "sftp" in self._application_config:
            sftp_config = self._application_config["sftp"]
            mandatory_params = ["host", "username", "password"]
            for sftp_name in sftp_config.keys():
                self._check_configuration_parameters(
                    sftp_name, sftp_config[sftp_name], mandatory_params
                )
            self._sftp_config = sftp_config

    def _prepare_azureml_configuration(self):
        """Prepare Azure ML configuration."""
        if "azureml" in self._application_config:
            azureml_config = self._application_config["azureml"]
            mandatory_params = [
                "subscription_id", "resource_group", "workspace_name",
                "cluster_name", "cluster_vm_size", "cluster_max_nodes"
            ]
            self._check_configuration_parameters("azureml", azureml_config, mandatory_params)
            self._azureml_config = azureml_config

    def _check_configuration_parameters(self, config_name, config_content, mandatory_params):
        """Check if the mandatory configuration parameters are filled in the application.yml file.

        Parameters
        ----------
        config_name : str
            Name of the configuration section in the application.yml file.
        config_content : dict
            Configuration filled in the application.yml file.
        mandatory_params : list
            List of mandatory configuration parameters.

        """
        for param in mandatory_params:
            if param not in config_content:
                raise KeyError(
                    f"Missing '{param}' definition in {config_name} configuration. "
                    f"Please fill it in {self._logging_conf_filepath}"
                )
