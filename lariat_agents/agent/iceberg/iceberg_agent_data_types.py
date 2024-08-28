from enum import Enum


class CatalogType(Enum):
    GLUE = "glue"
    REST = "rest"
    POSTGRES = "postgres"
    SQLLITE = "sqlite"
    HIVE = "hive"
    DYNAMODB = "dynamodb"
