import asyncio
import io
import requests
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


class Model(ABC):

    url = "https://analitika.woysa.club/images/panel/json/download/niches.php"

    @abstractmethod
    async def download_async(self, skip, categories):
        pass

    @abstractmethod
    def save_dict(self, data_list):
        pass


class Loader(Model):

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Loader, cls).__new__(cls)
        return cls._instance

    async def download_async(self, skip, categories):

        batch_size = min(len(categories), 3)
        category_batches = np.array_split(np.array(categories), batch_size)

        tasks = []
        for batch in category_batches:
            tasks.extend([self._fetch_category(skip, category) for category in batch])

        results = await asyncio.gather(*tasks)
        return results

    async def _fetch_category(self, skip, category):

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_download, skip, category)

    def _sync_download(self, skip, category):

        url = self.url + (
            f"?skip={skip}"
            "&price_min=0&price_max=1060225"
            "&up_vy_min=0&up_vy_max=108682515"
            "&up_vy_pr_min=0&up_vy_pr_max=2900"
            "&sum_min=1000&sum_max=82432725"
            "&feedbacks_min=0&feedbacks_max=32767"
            "&trend=false&sort=sum_sale&sort_dir=-1"
            f"&id_cat={category}"
        )
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
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


async def main():
    loader = Loader()
    categories = list(range(1, 20))
    skip_value = 1

    binary_data = await loader.download_async(skip_value, categories)

    transformed_data = loader.save_dict(binary_data)

    print(transformed_data)

if __name__ == "__main__":
    asyncio.run(main())
