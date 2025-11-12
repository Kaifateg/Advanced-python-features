import requests
import io
import pandas as pd
from abc import ABC, abstractmethod
from requests.exceptions import HTTPError


class Model(ABC):

    url = "https://analitika.woysa.club/images/panel/json/download/niches.php"

    @abstractmethod
    def download(self, skip, category):
        pass

    @abstractmethod
    def save_dict(self):
        pass


class Loader(Model):

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.obj = range(0, 10000000000)
            cls.instance = super(Loader, cls).__new__(cls)
        return cls.instance

    def __init__(self, response=None):
        self.response = response

    def download(self, skip, category):

        url = self.url + (f"?skip="
                          f"{skip}&price_min=0&price_max=1060225&up_vy_min=0"
                          f"&up_vy_max=108682515&up_vy_pr_min=0&up_vy_pr_max"
                          f"=2900&sum_min=1000&sum_max=82432725&feedbacks_min"
                          f"=0&feedbacks_max=32767&trend=false&sort=sum_sale"
                          f"&sort_dir=-1&id_cat={category}")

        try:
            self.response = requests.get(url, timeout=1)
            self.response.raise_for_status()
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")
        else:
            return self.response

    def save_dict(self):
        buffer = io.BytesIO
        df = pd.read_excel(buffer(self.response.content))
        result = df.to_dict(orient='dict')
        print(result)
        return result


loader = Loader()
loader.download(100, 10000)
loader.save_dict()



