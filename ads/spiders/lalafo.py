from datetime import datetime
from ads.items import HousingItems
import scrapy
import json

rent_house_id = 2032
rent_apartments_id = 2043
rent_room_id = 2051
bishkek_id = 103184

headers = {
    'accept': 'application/json',
    'accept - encoding': 'gzip, deflate, br',
    'accept - language': 'en-US,en;q=0.8,ru;q=0.9',
    'language': 'ru_RU',
    'device': 'pc',
}


class RealEstateSpider(scrapy.Spider):
    name = 'lalafo'
    allowed_domains = ['lalafo.kg']
    start_urls = [
        f'https://lalafo.kg/api/search/v3/feed/search?category_id={rent_house_id}&city_id={bishkek_id}',
        f'https://lalafo.kg/api/search/v3/feed/search?category_id={rent_apartments_id}&city_id={bishkek_id}',
        f'https://lalafo.kg/api/search/v3/feed/search?category_id={rent_room_id}&city_id={bishkek_id}',
        ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse,
            )

    def parse(self, response):
        yield scrapy.Request(
            url=response.url,
            headers=headers,
            callback=self.parse_api,
            dont_filter=True,
        )

    def parse_api(self, response):
        data = json.loads(response.body)
        for item in data['items']:
            id = item.get('id')
            yield scrapy.Request(
                url=f'https://lalafo.kg/api/search/v3/feed/details/{id}',
                callback=self.parse_details,
                headers=headers,
                meta={
                    'ad_label': item.get('ad_label'),
                }
            )

        next_page = data.get('_links').get('next')
        if next_page is not None:
            url = 'https://lalafo.kg/api/search/v3/feed/' + next_page.get('href')[8:]
            yield scrapy.Request(
                url=url,
                callback=self.parse_api,
                headers=headers,
            )

    def parse_details(self, response):
        item = json.loads(response.body)
        items = HousingItems()

        items['site'] = 'https://lalafo.kg/'
        items['category'] = response.meta.get('ad_label')
        items['title'] = item.get('title')
        items['price'] = item.get('price')
        items['currency'] = item.get('currency')
        items['description'] = item.get('description')
        items['parse_datetime'] = datetime.now()
        items['ad_url'] = item.get('url')

        yield items
