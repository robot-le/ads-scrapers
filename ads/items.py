# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class HousingItems(scrapy.Item):
    site = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    currency = scrapy.Field()
    description = scrapy.Field()
    parse_datetime = scrapy.Field()
    ad_url = scrapy.Field()
    daily = scrapy.Field()
    address = scrapy.Field()
    additional = scrapy.Field()
    images = scrapy.Field()
    rooms = scrapy.Field()
    apartment_area = scrapy.Field()
    land_area = scrapy.Field()
    series = scrapy.Field()
    furniture = scrapy.Field()
    renovation = scrapy.Field()
    pets = scrapy.Field()
    seller = scrapy.Field()

