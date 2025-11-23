import asyncio
import time
import numpy as np
import requests
import io
import pandas as pd
import concurrent.futures
from abc import ABC, abstractmethod


class Model(ABC):
    url = "https://analitika.woysa.club/images/panel/json/download/niches.php"

    @abstractmethod
    def download(self, skip, category):
        pass

    @abstractmethod
    def save_dict(self, data_list):
        pass


class Loader(Model):
    _instance = None
    results = []

    def new(cls):
        if cls._instance is None:
            cls._instance = super(Loader, cls).new(cls)
        return cls._instance

    def start(self):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pool.map(self.export_batch, np.array_split(range(0, 100), 10))

        print(self.results)

    def export_batch(self, categories):
        asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.get_event_loop().run_until_complete(asyncio.gather(*[
            asyncio.ensure_future(self.export_date(category))
            for category in categories
        ]))

    async def export_date(self, category):
        data_list = await self.load_data(category)
        results = self.save_dict(data_list)
        self.results.extend(results)

    async def load_data(self, category):
        data_list = []
        for skip in range(0, 99, 100):
            content = self.download(skip, category)
            if not content or content == b'':
                break
            data_list.append(content)
        return data_list

    def download(self, skip, category, retry_count=5):
        url = self.url + (
            f"?skip={skip}"
            f"&price_min=0&price_max=1060225"
            f"&up_vy_min=0&up_vy_max=108682515"
            f"&up_vy_pr_min=0&up_vy_pr_max=2900"
            f"&sum_min=1000&sum_max=82432725"
            f"&feedbacks_min=0&feedbacks_max=32767"
            f"&trend=false&sort=sum_sale"
            f"&sort_dir=-1&id_cat={category}"
        )

        try:
            response = requests.get(url, timeout=1)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            if retry_count == 0:
                return True
            time.sleep(10)
            self.download(skip, category, retry_count - 1)
            print(f"Ошибка при скачивании данных: {e}")
            return b''

    def save_dict(self, data_list):
        results = []
        for data in data_list:
            if len(data) > 0:
                df = pd.read_excel(io.BytesIO(data))
                result = df.to_dict(orient="dict")
                results.append(result)
            else:
                print("Нет данных для обработки.")
        return results


loader = Loader()
loader.start()
