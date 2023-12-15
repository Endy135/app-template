"""SFTPClient class."""

import os
import logging
import pysftp
import re
import numpy as np
from datetime import datetime

from ..configuration.app import AppConfig


class SftpClient(AppConfig):
    """Class for SFTP Client."""

    # SFTP Connection
    _cnopts = pysftp.CnOpts()
    _cnopts.hostkeys = None

    def __init__(self, local_dirname="sftp"):
        """Initialise SFTPClient.

        Parameters
        ----------
        local_sftp_dirname : str
            Name of the directory in 'data' directory where the files transferred
            from/to the sftp will be stored.

        """
        AppConfig.__init__(self)
        self._server_names = list(self._sftp_config.keys())
        self._prepare_sftp_target_dirpath(local_dirname)
        self._last_downloaded_filenames = []

    @property
    def server_names(self):
        """Return local sftp dirpath."""
        return self._server_names

    @property
    def sftp_target_dirpath(self):
        """Return local sftp dirpath."""
        return self._sftp_target_dirpath

    @property
    def last_downloaded_filenames(self):
        """Return files downloaded from last call download_from_sftp."""
        return self._last_downloaded_filenames

    def server_listdir(self, server_name, server_dirpath="./", sort=True):
        """Get all objects present in a sftp directory.

        Parameters
        ----------
        server_name : str
            Name of sftp server.
        server_dirpath : str
            Path to the directory of the sftp server where the objects are stored.
        sort : bool
            Whether to sort by last modified date or not.

        Returns
        -------
        dict
            Keys are objects names and values are dictionaries with the following structure:
                lm_date: datetime
                    Last modified date.
                is_directory: bool
                    Whether is a directory or a file.
        """
        if server_name not in self._sftp_config.keys():
            raise (
                KeyError(
                    f"Missing {server_name} in application configuration."
                    f" List of known sftp servers {self._sftp_config.keys()}"
                )
            )

        with pysftp.Connection(
            **self._sftp_config[server_name], cnopts=self._cnopts
        ) as sftp:
            if not sftp.exists(server_dirpath):
                raise FileExistsError(f"{server_dirpath} directory doesn't exist")

            sftp.cwd(server_dirpath)
            # List all files
            sftp_files = {}
            filedates = []
            for file in sftp.listdir_attr():
                filedate = datetime.fromtimestamp(file.st_mtime)
                sftp_files[file.filename] = {
                    "lm_date": filedate,
                    "is_directory": sftp.isdir(file.filename),
                }
                filedates += [filedate]
            # Sort by last modified date
            if sort:
                files_sort = [
                    f for d, f in sorted(zip(filedates, sftp_files), reverse=True)
                ]
                sftp_files = {f: sftp_files[f] for f in files_sort}

            return sftp_files

    def upload_files(self, server_name, filenames, server_dirpath="./", overwrite=False):
        """Put local files to sftp server.

        Parameters
        ----------
        server_name : str
            Name of sftp server.
        filenames : str or list
            Filenames to upload from local sftp directory to sftp server.
        server_dirpath : str
            Path to the directory of the sftp server where the files are stored.
        overwrite : bool
            Whether to overwrite file in stfp server or not.
        """
        if server_name not in self._sftp_config.keys():
            raise (
                KeyError(
                    f"Missing {server_name} in application configuration."
                    f" List of known sftp servers {self._sftp_config.keys()}"
                )
            )
        if not isinstance(filenames, list):
            filenames = [filenames]

        with pysftp.Connection(
            **self._sftp_config[server_name], cnopts=self._cnopts
        ) as sftp:
            # Go to sftp_dirpath & create it if doesn't exist
            if not sftp.exists(server_dirpath):
                sftp.mkdir(server_dirpath)

            sftp.cwd(server_dirpath)

            # Upload files
            for filename in filenames:
                logging.info(f"Putting {filename} to the {server_name} sftp server")

                # Check if filename is in local
                if filename not in os.listdir(self._sftp_target_dirpath):
                    raise FileExistsError(
                        f"{filename} file is not present in local directory"
                    )
                # Check if filename is in server
                if filename in sftp.listdir() and not overwrite:
                    raise FileExistsError(
                        f"{filename} file already exists in SFTP server."
                        f"To overwritte {filename} use argument overwrite"
                    )

                # local_sftp_filepath
                local_sftp_filepath = os.path.join(self._sftp_target_dirpath, filename)

                # Put to sftp server
                sftp.put(local_sftp_filepath, filename)

    def download_files(
        self,
        server_name,
        server_dirpath="./",
        filename_patterns=None,
        keep_only_last=False,
        overwrite=False
    ):
        """Get local file from sftp server.

        Parameters
        ----------
        server_name : str
            Name of sftp server.
        server_dirpath : str
            Path to the directory of the sftp server where the files are stored.
        filename_patterns : str or list or None
            Patterns that sftp server content have to contain. It can be a regex.
        keep_only_last : bool
            Whether to take only last modified file or all.
        overwrite : bool
            Whether to overwrite file in stfp server or not.
        """
        if server_name not in self._sftp_config.keys():
            raise (
                KeyError(
                    f"Missing {server_name} in application configuration."
                    f" List of known sftp servers {self._sftp_config.keys()}"
                )
            )
        if filename_patterns is None:
            filename_patterns = [""]
        if not isinstance(filename_patterns, list):
            filename_patterns = [filename_patterns]

        # Get server filenames
        filenames = self.server_listdir(
            server_name, server_dirpath, sort=keep_only_last
        )

        with pysftp.Connection(
            **self._sftp_config[server_name], cnopts=self._cnopts
        ) as sftp:
            # Go to sftp_dirpath
            if not sftp.exists(server_dirpath):
                raise FileExistsError(f"{server_dirpath} directory doesn't exist")

            sftp.cwd(server_dirpath)
            self._last_downloaded_filenames = []

            for pat in filename_patterns:
                # Filter content
                filenames_filter = [
                    f
                    for f in filenames.keys()
                    if bool(re.search(pat, f)) and not filenames[f]["is_directory"]
                ]
                # Check if some contents with the pattern is in server
                if len(filenames_filter) == 0:
                    raise FileExistsError(
                        f"No file present in the SFTP server with the pattern: {pat}"
                    )
                # Check if it is already in local
                if (
                    np.any([f in os.listdir(self._sftp_target_dirpath) for f in filenames_filter])
                    and not overwrite
                ):
                    raise FileExistsError(
                        f"Some files with the pattern {pat} already exists in local directory."
                        f"To overwritte file, use argument overwrite"
                    )

                if keep_only_last:
                    filenames_filter = [filenames_filter[0]]

                for sftp_filename in filenames_filter:
                    logging.info(
                        f"Getting {sftp_filename} from the {server_name} sftp server"
                    )

                    # Get from sftp server
                    local_sftp_filepath = os.path.join(self._sftp_target_dirpath, sftp_filename)
                    sftp.get(sftp_filename, local_sftp_filepath)

                    self._last_downloaded_filenames += [sftp_filename]

    def _prepare_sftp_target_dirpath(self, local_dirname):
        self._sftp_target_dirpath = os.path.join(os.environ["UNXDATA"], local_dirname)
        if not os.path.exists(self._sftp_target_dirpath):
            os.makedirs(self._sftp_target_dirpath)
