from abc import ABC, abstractmethod


class StreamingBaseQueryBuilder(ABC):
    """
    This class provides the expected interface to:
    - Take a group of indicators and return the actual query run against a database or data warehouse
    - Retrieve the schema from the database or data warehouse

    The implementor has the responsibility of ensuring that the data source works with both sketches and non-sketch
    modes, or failing initializtion of the query builder if a mode isn't supported.
    """

    def __init__(self, query_builder_type: str, sketch_mode: bool = True):
        """
        :param query_builder_type: Name of the query builder type
        :param sketch_mode: boolean indicating whether sketches or raw values should be returned
        """
        self._query_builder_type = query_builder_type
        # TODO: Create a property that tracks sketch_mode
        self.sketch_mode = sketch_mode

    def __str__(self):
        return self._query_builder_type

    @abstractmethod
    def run(
        self,
        fsspec_name,
        partition_fields_in_data,
        clean_schema,
        file_type,
        string_columns,
        numeric_columns,
        timestamp_mappings,
        dimensions,
        source_id,
        dataset_name,
        location_info,
        content_length,
    ):
        """
        Logic to execute the query against the data source (e.g. execute the actual kafka query or csv read or
        parquet file read from s3)
        :param fsspec_name:
        :param partition_fields_in_data:
        :param clean_schema:
        :param file_type:
        :param string_columns:
        :param numeric_columns:
        :param timestamp_mappings:
        :param dimensions:
        :param source_id:
        :param dataset_name:
        :param location_info:
        :param content_length:
        :return:
        """
        pass
