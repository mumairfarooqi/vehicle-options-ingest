from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import csv
import json
from typing import Dict

from airflow.operators import BaseOperator

import pathlib
from string import Template as DefaultTemplate

WILDCARD = "*"


class GcstoGcsWithDestFilename(GoogleCloudStorageToGoogleCloudStorageOperator):
    @apply_defaults
    def __init__(self, destination_object: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.destination_object = destination_object

    def execute(self, context):

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to,
        )

        if self.destination_bucket is None:
            self.log.warning(
                "destination_bucket is None. Defaulting it to source_bucket (%s)",
                self.source_bucket,
            )
            self.destination_bucket = self.source_bucket

        if WILDCARD in self.source_object:
            total_wildcards = self.source_object.count(WILDCARD)
            if total_wildcards > 1:
                error_msg = (
                    "Only one wildcard '*' is allowed in source_object parameter. "
                    "Found {} in {}.".format(total_wildcards, self.source_object)
                )

                raise AirflowException(error_msg)

            prefix, delimiter = self.source_object.split(WILDCARD, 1)
            objects = hook.list(self.source_bucket, prefix=prefix, delimiter=delimiter)

            for source_object in objects:
                if self.destination_object is None:
                    destination_object = source_object
                else:
                    destination_object = self.destination_object

                self._copy_single_object(
                    hook=hook,
                    source_object=source_object,
                    destination_object=destination_object,
                )
        else:
            self._copy_single_object(
                hook=hook,
                source_object=self.source_object,
                destination_object=self.destination_object,
            )


class CSVtoJsonOperator(BaseOperator):
    """

    input and output files are both located in the local file system.

    """

    template_fields = ("input_filename", "output_filename")

    @apply_defaults
    def __init__(
        self, input_filename: str, output_filename: str, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.input_filename = input_filename
        self.output_filename = output_filename

    def output_columns(self, json_content: Dict[str, object]) -> Dict[str, object]:
        """
        Allows to add additional columns to the output using custom parsing logic. The content field
        will be left unchanged
        :param json_content: Parsed content of each row
        :return: Dictionary containing additional attribute - value pairs
        """
        return {}

    def execute(self, context):

        with open(self.input_filename, "r") as csvfile:
            dialect = csv.Sniffer().sniff(csvfile.read(16384), ",\t; :|")
            csvfile.seek(0)  # back to top
            reader = csv.DictReader(csvfile, dialect=dialect)

            # Some CSVs contain BOM character at the beginning. It needs utf-8-sig encoding to read.
            # We always read in utf-8 format which prepends this character in text content.
            # Thus we need to remove it from text stream
            first_character = csvfile.read(1)
            if not first_character == "\ufeff":
                # When first character is not BOM, move position to start
                csvfile.seek(0)
            with open(self.output_filename, "w") as jsonfile:
                for row in reader:
                    output_row = self.output_columns(row)
                    output_row.update({"content": json.dumps(row, ensure_ascii=False)})

                    jsonfile.write(json.dumps(output_row, ensure_ascii=False))
                    jsonfile.write("\n")

        return f"{self.input_filename} converted to json"


# Gives abs path to project module, eg foo-ingest has module foo_ingest
PROJECT_MODULE_PATH = pathlib.Path(__file__).parent.parent.parent.absolute()


def read_query(rel_path, params=None):
    absolute_path = f"{PROJECT_MODULE_PATH}/{rel_path}"

    with open(absolute_path, "r") as f:
        if not params:
            return f.read()

        t = DefaultTemplate(f.read())
        return t.safe_substitute(params)
