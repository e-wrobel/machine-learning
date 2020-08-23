import os

import regex as regex
import requests
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class LoadData(object):

    def __init__(self, logger, load_data=True):
        self.logger = logger
        self.load_data = load_data

    def fetch_data(self, data_definition: dict, root_directory='/var/tmp/data'):
        """
        It is intended to gather data from internet and store it into filesystem.

        :param data_definition: Dictionary containing key as category, and list as urls
        :param root_directory: Defines where to save files

        :rtype: bool
        """

        if not self.load_data:
            self.logger.info("Not loading data into: {}".format(root_directory))
            return True

        self.logger.info("Checking if root data exists")
        if not self._check_directory_exists(root_directory):
            self.logger.error("Filesystem issue with root directory: {}".format(root_directory))

        self.logger.info("Saving files...")
        for category, urls in data_definition.items():
            self._save_single(category=category, urls=urls, directory=root_directory)

    def _save_single(self, category, urls, directory):
        """
        Savesingle category into filesystem.

        :param category: Category
        :param urls: List of urls for selected category
        :param directory: Base root_directory

        :rtype: bool
        """

        path_to_files_in_category = "{}/{}".format(directory, category)
        if not self._check_directory_exists(path_to_files_in_category):
            self.logger.error("Unable to save files for {} category, filesystem error".format(category))

        for url in urls:
            file_for_url = "{}/{}".format(path_to_files_in_category, url.split('/')[-1])
            with open(file_for_url, mode='w', encoding='utf-8') as f:
                self.logger.debug("Saving: {} to: {}".format(url, file_for_url))
                data = requests.get(url, verify=False)
                data = data.text
                self.logger.debug("Filtering data")
                filtered_data = self._filter_data(data)
                f.write(filtered_data)
                self.logger.debug("Data saved")

    def _filter_data(self, data: str):
        """
        Remove blank lines, formatting etc.

        :param data: Data string
        :return:
        """

        text = regex.sub(u"[^ \n\p{Latin}\-'.?!]", " ", data)
        text = regex.sub(u"[ \n]+", " ", text)  # Squeeze spaces and newlines
        text = regex.sub(r"----- ta lektura.*", "", text)  # remove footer

        out = [regex.sub(r"^ ", "", l) for l in regex.split('\.|,|\?|!|:', text)]

        return ''.join(out).lower()

    def _check_directory_exists(self, directory: str):
        """
        Check if directory exists and create if does not.

        :param directory: Directory
        :rtype: bool
        """

        if not os.path.exists(directory):
            try:
                os.mkdir(directory)
                self.logger.info("Directory: {} created".format(directory))
                return True
            except OSError:
                self.logger.error("Creation of the directory {} failed".format(directory))
                return False

        self.logger.info("Directory {} exists".format(directory))
        return True
