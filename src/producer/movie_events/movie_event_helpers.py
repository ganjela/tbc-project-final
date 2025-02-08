from datetime import datetime
from typing import Dict, Union, Any
from pyspark.sql import Row
from confluent_kafka.serialization import SerializationContext

def movie_row_to_dict(row: Row) -> Dict[str, Union[str, int, float]]:
    """
    Converts a PySpark Row object into a dictionary representation of a movie event.

    :param row: A PySpark Row object containing movie details.
    :return: A dictionary with movie event details.
    """
    return {
        "event_name": "movies_catalog_enriched",
        "movie_id": row.movie_id,
        "title": row.Title,
        "parsed_price": row.parsed_price,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def movie_to_dict(data: Dict[str, Any], ctx: SerializationContext) -> Dict[str, Union[str, int, float]]:
    """
    Converts a dictionary representation of a movie event for serialization.

    :param data: A dictionary containing movie event details.
    :param ctx: The serialization context from Confluent Kafka.
    :return: A dictionary formatted for Avro serialization.
    """
    return {
        "event_name": data.get("event_name", ""),
        "movie_id": data.get("movie_id", 0),
        "title": data.get("title", ""),
        "parsed_price": data.get("parsed_price", 0.0),
        "event_time": data.get("event_time", "")
    }
