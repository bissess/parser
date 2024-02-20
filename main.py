import time

from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import requests
import asyncio
import aiohttp

Base = declarative_base()


class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    category = Column(String)
    product_name = Column(String)
    product_link = Column(String)
    product_price = Column(Float)


class Category:

    def __init__(self, name, start_page=None, end_page=None):
        self.name = name
        self.base_url = 'https://omarket.kz/catalog/ecc_kompyutery/ecc_noutbuk/'
        self.engine = create_engine('postgresql://bissess:bmwm4f82@localhost:5432/parser')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.start_page = start_page
        self.end_page = end_page

        self.headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }

    def insert_data(self, category, product_name, product_link, product_price):
        # Checking for the existing data
        with self.Session() as session:
            # Choose, which rows must be checked
            existing_product = session.query(Product).filter(
                (Product.category == category) &
                (Product.product_name == product_name) &
                (Product.product_link == product_link) &
                (Product.product_price == product_price)
            ).first()

            # if data already exists - update
            if existing_product:
                existing_product.category = category
                existing_product.product_name = product_name
                existing_product.product_link = product_link
                existing_product.product_price = product_price
            # Else, we add this data
            else:
                new_product = Product(category=category,
                                      product_name=product_name,
                                      product_link=product_link,
                                      product_price=product_price)
                session.add(new_product)

            session.commit()

    async def parse_products(self, category_path=''):
        await self.parse_products_async(category_path)

    async def fetch_page(self, session, category_path):
        url = f'{self.base_url}{category_path}'
        async with session.get(url, headers=self.headers) as response:
            if response.status == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type:
                    return await response.text()
                else:
                    print(f"Ошибка загрузки страницы {url}. Некорректный тип содержимого: {content_type}")
            print(f"Ошибка загрузки страницы {url}. Статус: {response.status}")

    async def parse_products_async(self, category_path=''):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_page(session, f'{category_path}?PAGE=page-{page_number}')
                     for page_number in range(self.start_page, self.end_page + 1)]
            pages = await asyncio.gather(*tasks)

            for page in pages:
                soup = BeautifulSoup(page, 'lxml')
                if soup is None:
                    print(f'Не удалось получить данные из страницы {self.url}')
                    continue

                products = soup.find_all('div', class_='productColText')
                if not products:
                    print(f'На странице {self.url} не найдено продуктов')
                    continue

                for product in products:
                    product_name = product.find_all('span', {'class': 'middle'})
                    product_link = product.find_all('a', {'class': 'name'})
                    product_price = product.find_all('a', {'class': 'price'})

                    if product_name and product_link and product_price:
                        for name, link, price in zip(product_name, product_link, product_price):
                            cleaned_price_str = price.get_text(strip=True).replace(' ', '').replace('\n', '')
                            cleaned_price = float(cleaned_price_str.replace('\xa0', '').replace('тенге', ''))
                            self.insert_data(self.name, name.text, link['href'], cleaned_price)
            return True


class LaptopBatteries(Category):
    def __init__(self):
        super().__init__('Laptop Batteries', start_page=1, end_page=13)

    async def parse_products(self):
        await super().parse_products('/laptop-batteries/')


class LaptopPowerSupplies(Category):
    def __init__(self):
        super().__init__('Laptop Power Supplies', start_page=1, end_page=21)

    async def parse_products(self):
        await super().parse_products('/power-supplies-for-laptops/')


class LaptopDocStations(Category):
    def __init__(self):
        super().__init__('Laptop Doc Stations', start_page=1, end_page=9)

    async def parse_products(self):
        await super().parse_products('/dok-stantsii-dlya-noutbukov/')


class LaptopLocks(Category):
    def __init__(self):
        super().__init__('Laptop Locks', start_page=1, end_page=1)

    async def parse_products(self):
        await super().parse_products('/zamki-dlya-noutbukov/')


class LaptopMatrices(Category):
    def __init__(self):
        super().__init__('Laptop Matrices', start_page=1, end_page=1)

    async def parse_products(self):
        await super().parse_products('/matritsy-dlya-noutbukov/')


class Laptops(Category):
    def __init__(self):
        super().__init__('Laptops', start_page=1, end_page=249)

    async def parse_products(self):
        await super().parse_products('/noutbuki/')


class LaptopStands(Category):
    def __init__(self):
        super().__init__('Laptop Stands', start_page=1, end_page=8)

    async def parse_products(self):
        await super().parse_products('/podstavki_dlya_noutbuka/')


class LaptopBags(Category):
    def __init__(self):
        super().__init__('Laptop Bags', start_page=1, end_page=83)

    async def parse_products(self):
        await super().parse_products('/sumki_dlya_noutbukov/')


start_time = time.time()


async def main():
    laptop_batteries = LaptopBatteries()
    await laptop_batteries.parse_products()

    laptop_power_supplies = LaptopPowerSupplies()
    await laptop_power_supplies.parse_products()

    laptop_doc_stations = LaptopDocStations()
    await laptop_doc_stations.parse_products()

    laptop_locks = LaptopLocks()
    await laptop_locks.parse_products()

    laptop_matrices = LaptopMatrices()
    await laptop_matrices.parse_products()

    laptops = Laptops()
    await laptops.parse_products()

    laptop_stands = LaptopStands()
    await laptop_stands.parse_products()

    laptop_bags = LaptopBags()
    await laptop_bags.parse_products()

    laptop_batteries.engine.dispose()
    laptop_power_supplies.engine.dispose()
    laptop_doc_stations.engine.dispose()
    laptop_locks.engine.dispose()
    laptop_matrices.engine.dispose()
    laptops.engine.dispose()
    laptop_stands.engine.dispose()
    laptop_bags.engine.dispose()

if __name__ == '__main__':
    asyncio.run(main())

end_time = time.time()
execution_time = end_time - start_time
print(f"Время выполнения программы: {execution_time} секунд")