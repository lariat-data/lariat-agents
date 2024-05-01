from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class BaseSink(ABC):
    """
    Class to implement in order to write to various sources.
    This class is only re-implemented when the desired outcome is to bypass the Lariat Service entirely and write to
    Datadog or Grafana directly.
    """

    def __init__(self, source_cloud: str, supports_tags: bool = False):
        """
        :param source_cloud: cloud environment that the batch_base sink is operating in
        :param supports_tags: boolean indicating whether the sink supports tagging
        """
        self.source_cloud = source_cloud
        self.supports_tags = supports_tags

    @abstractmethod
    def write(
        self,
        result_df: pd.DataFrame = None,
        source_top_level: str = None,
        file_path: str = None,
        tags_dict: Dict = None,
    ):
        """
        :param result_df:
        :param source_top_level:
        :param file_path:
        :param tags_dict:
        :return:
        """
