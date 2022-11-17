from datetime import datetime
from ads.items import HousingItems
import scrapy


class HouseSpider(scrapy.Spider):
    name = 'house'
    allowed_domains = ['house.kg']
    start_urls = [
        'https://www.house.kg/snyat-kvartiru?region=1&town=2',
        'https://www.house.kg/snyat-dom?region=1&town=2',
    ]

    def parse(self, response):
        ads = response.xpath('//div[@itemtype="https://schema.org/Apartment"]')
        if 'snyat-dom' in response.url:
            ads = response.xpath('//div[@itemtype="https://schema.org/House"]')

        for ad in ads:
            url = 'https://www.house.kg' + ad.xpath('./meta[@itemprop="url"]/@content').get()
            rooms = ad.xpath('./meta[@itemprop="numberOfRooms"]/@content').get()
            category = ad.xpath('./meta[@itemprop="name"]/@content').get()
            address = ad.xpath('./meta[@itemprop="address"]/@content').get()
            description = ad.xpath('./meta[@itemprop="description"]/@content').get()
            yield scrapy.Request(
                url=url,
                callback=self.parse_ad,
                meta={
                    'rooms': rooms,
                    'category': category,
                    'address': address,
                    'description': description,
                }
            )

        next_page = response.xpath('//a[@aria-label="Вперед"]/@href').get()
        if 'page=' in next_page:
            yield scrapy.Request(
                url='https://www.house.kg' + next_page,
                callback=self.parse,
            )

    def parse_ad(self, response):
        items = HousingItems()
        additional = {}
        for row in response.xpath('//div[@class="info-row"]'):
            if row.xpath('./div[1]/text()').get().strip() == 'Тип предложения':
                continue
            additional[f'''{row.xpath('./div[1]/text()').get().strip()}'''] = row.xpath('./div[2]/text()').get().strip()

        rental_period = additional.get('Период аренды')

        if rental_period is not None and rental_period.lower().strip() == 'посуточно':
            items['daily'] = True
        elif rental_period is None:
            items['daily'] = None
        else:
            items['daily'] = False

        items['site'] = 'house.kg'
        items['category'] = response.meta.get('category')
        items['title'] =  response.xpath('//title/text()').get()
        items['price'] = response.xpath('//div[@class="price-som"]/text()').get().replace('сом', '').replace(' ', '')
        items['currency'] = 'KGS'
        items['description'] = response.meta.get('description')
        items['parse_datetime'] = datetime.now()
        items['ad_url'] = response.url
        items['address'] = response.meta.get('address')
        items['additional'] = '\n'.join([f'{key}: {value}' for key, value in additional.items()])
        images = response.xpath('//div[@class="fotorama"]/a/@data-full').getall()
        items['images'] = images
        # items['price_usd'] = response.xpath('//div[@class="price-dollar"]/text()').get().replace('$', '').replace(' ', '')
        # items['rooms'] = response.meta.get('rooms')

        yield items
