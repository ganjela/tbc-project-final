from datetime import datetime
from typing import Dict, Union

class MovieCatalogEnriched:
    def __init__(self, movie_id: int, title: str, parsed_price: float):
        self.movie_id = movie_id
        self.title = title
        self.parsed_price = parsed_price
        self.event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def __call__(self) -> Dict[str, Union[str, int, float]]:
        return {
            "event_name": "movies_catalog_enriched",
            "movie_id": self.movie_id,
            "title": self.title,
            "parsed_price": self.parsed_price,
            "event_time": self.event_time
        }

    @staticmethod
    def to_dict(data, ctx) -> Dict[str, Union[int, str, float]]:
        return {
            "event_name": data.get("event_name"),
            "movie_id": data.get("movie_id"),
            "title": data.get("title"),
            "parsed_price": data.get("parsed_price"),
            "event_time": data.get("event_time")
        }

