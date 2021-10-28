
import json
import os

from definitions import LOG_FILE
from toolkit.logging import logger


class MetadataManager(object):

    """
    Easy metadata management for Revan
    """

    STAGE_ORDER = ("fetch", "munge", "standardize", "transform",
                   "fragment", "resolve", "map", "reduce", "write")

    def __init__(self):
        self.log_file = self.create_file()
        self.metadata = self.read_metadata()

    def search_metadata(self, *args, **kwargs):
        meta = kwargs.get("meta", self.metadata)

        if not args:
            return meta

        if args[0] in meta:
            return self.search_metadata(*args[1:], meta=meta[args[0]])

        return False

    @staticmethod
    def create_file():
        """
        Creates the file, if it hasn't already been.

        Returns:
            (str) absolute path to the file
        """
        if not os.path.exists(LOG_FILE):
            open(LOG_FILE, 'w+').close()
            logger.debug(f"Created metadata file at {LOG_FILE}")

        return LOG_FILE

    def write_metadata(self, obj):
        """
        Updates metadata on self and writes the changes to file.

        Args:
            obj (dict): metadata to be written

        Returns:
            True
        """
        updated = self.merge_dict_structures(self.metadata, obj)

        with open(LOG_FILE, 'w+') as file:
            file.write(json.dumps(updated, indent=4, sort_keys=True))

        self.metadata = updated

        return True

    @staticmethod
    def read_metadata():
        """
        Reads the metadata file
        Args:
            obj (dict): metadata to be written

        Returns:
            True
        """

        metadata = open(LOG_FILE, 'r').read()
        return json.loads(metadata) if metadata else {}

    @staticmethod
    def write_json(obj, path):
        with open(path, 'w+') as file:
            file.write(json.dumps(obj, indent=2, sort_keys=True))

        return True

    def merge_dict_structures(self, a, b, _path=None):
        """
        Merges two JSON structures

        Args:
            a: first JSON structure
            b: second JSON structure
            _path: Do not use. Private variable to keep track of location in JSON

        Returns:
            Dictionary structure comprised of all key/values from both A and B
        """
        _path = _path or []
        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    self.merge_dict_structures(a[key], b[key], _path + [str(key)])

                else:
                    a[key] = b[key]
            else:
                a[key] = b[key]
        return a


    #def verify_prerequisite(self, stage):
