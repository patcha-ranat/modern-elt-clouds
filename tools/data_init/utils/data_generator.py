from typing import Any
import datetime
import requests


class RandomUserSchema:
    
    def __init__(self, data: dict[str, Any]) -> None:
        self.login_uuid: str = data.get("login").get("uuid")
        self.name_title: str = data.get("name").get("title")
        self.name_first: str = data.get("name").get("first")
        self.name_last: str = data.get("name").get("last")
        self.gender: str = data.get("gender")
        self.date_of_birth: datetime = data.get("dob").get("date")
        self.user_age_year: int = data.get("dob").get("age")
        self.email: str = data.get("email")
        self.phone: str = data.get("phone")
        self.picture: str = data.get("picture").get("large")
        self.nationality: str = data.get("nat")
        self.location_city: str = data.get("location").get("city")
        self.location_state: str = data.get("location").get("state")
        self.location_country: str = data.get("location").get("country")
        self.location_postcode: str = data.get("location").get("postcode")
        self.location_coordinates_latitude: float = data.get("location").get("coordinates").get("latitude")
        self.location_coordinates_longitude: float = data.get("location").get("coordinates").get("longitude")
        self.login_username: str = data.get("login").get("username")
        self.login_password: str = data.get("login").get("password")
        self.registered_date: datetime = data.get("registered").get("date")
        self.registered_age_year: int = data.get("registered").get("age")
        self._data: dict[str, Any] = {
            "login_uuid": self.login_uuid,
            "name_title": self.name_title,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "gender": self.gender,
            "date_of_birth": self.date_of_birth,
            "user_age_year": self.user_age_year,
            "email": self.email,
            "phone": self.phone,
            "picture": self.picture,
            "nationality": self.nationality,
            "location_city": self.location_city,
            "location_state": self.location_state,
            "location_country": self.location_country,
            "location_postcode": self.location_postcode,
            "location_coordinates_latitude": self.location_coordinates_latitude,
            "location_coordinates_longitude": self.location_coordinates_longitude,
            "login_username": self.login_username,
            "login_password": self.login_password,
            "registered_date": self.registered_date,
            "registered_age_year": self.registered_age_year
        }
    
    def __str__(self) -> str:
        schema: str = str(self._data)
        return schema
    
    @property
    def data(self) -> dict[str, Any]:
        return self._data


class DataGeneratorAPI:
    """
    Random User API check the link for more info: https://randomuser.me/documentation#intro
    For simple use case, please check these parameters:

    :param results: number of records returned
    :type results: int
    :param seed: can be string or sequence of characters to generate the same set of users (limit randomness)
    :type seed: str | int
    :param format: returned format [json, csv, yaml, xml] (default json)
    :type format: str

    Example:
        data = DataGenerator(results=10).get_data()
    """

    def __init__(
        self,
        results: int = 10,
        format: str | None = "json",
        seed: int | str | None = None,
    ) -> None:
        self.base_url = "https://randomuser.me/api/"
        self.results = results
        self.format = format
        self.seed = seed
        self.data: dict = None

    def formulate_url(self):
        """
        Formulate URL according to query parameters
        """
        if self.seed:
            self.url = (
                self.base_url
                + f"?results={self.results}&format={self.format}&seed={self.seed}"
            )
        else:
            self.url = self.base_url + f"?results={self.results}&format={self.format}"

    @staticmethod
    def request_batch(url: str) -> requests.Response:
        """
        Get response from REST API

        :param url: ready URL for request
        :type url: str

        Return: response

        """
        response = requests.get(url)
        return response

    def get_data(self) -> list[dict[str, Any]]:
        """
        Class Entrypoint, Getting data from API response
        """
        if self.data:
            return self.data
        else:
            self.formulate_url()
            response = self.request_batch(url=self.url)
            self.data = response.json()["results"]
            return self.data

    def get_data_with_schema(self) -> list[RandomUserSchema]:
        """
        Alternative Class Entrypoint, Getting data from API response with pre-defined schema
        """
        if self.data:
            json_data = self.data
        else:
            json_data = self.get_data()

        records: list = []
        for record in json_data:
            records.append(RandomUserSchema(data=record).data)
        return records