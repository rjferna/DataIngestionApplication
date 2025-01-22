import configparser


class Config:

    REQUIRED_KEYS = [
        "host",
        "database",
        "user",
        "password",
        "gcp_creds",
        "archive_path",
        "file_path",
        "log_path",
        "log_bucket",
        "log_bucket_workflow_execution_destination",
    ]

    def __init__(self, config_file):
        self.config_file = config_file
        self.cp = configparser.ConfigParser()
        self.cp.read(config_file)
        self.required_keys = set(Config.REQUIRED_KEYS)

    def _get_common(self):
        config = {}
        items = self.cp.items("COMMON")
        for item in items:
            config[item[0]] = item[1]
        return config

    def _get_section(self, section_name):
        config = {}
        items = self.cp.items(section_name)
        for item in items:
            config[item[0]] = item[1]
        return config

    def set_value_by_section(self, section, key, value):
        if not (self.cp.has_section(section)):
            raise RuntimeError("Cannot find section %s" % (section))
        self.cp.set(section, key, value)
        self.cp.write(open(self.config_file, "w"))

    def get(self, section_name):
        """
        Returns all the configured attributes for section {section_name}, including the attributes in COMMON section

        Args: section_name: str. The section name of the configuration.
        Returns:
            dict. All the attributes configured in section {section_name} and COMMON.
            If a attribute is present in both sections, the non-empty one will be returned,
            if both are non-empty, return the section one. Other wise return None
        """

        common = self._get_common()
        section = self._get_section(section_name)

        # merge the two section
        config_attr = section
        for attr, value in common.items():
            if attr not in config_attr:
                config_attr[attr] = value
            else:
                if not config_attr[attr]:
                    config_attr[attr] = value

        # Check whether the required attributes are configured
        empty_required = [
            key
            for key in self.required_keys
            if key not in config_attr or not config_attr[key]
        ]
        if empty_required:
            raise ConfigIncompleteError(
                'Attributes "{}" need to be configured in file {}'.format(
                    '", "'.join(empty_required), self.config_file
                )
            )

        return config_attr


class ConfigIncompleteError(Exception):
    pass


if __name__ == "__main__":
    pass
