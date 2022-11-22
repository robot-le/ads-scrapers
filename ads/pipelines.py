from itemadapter import ItemAdapter
import psycopg2
from dotenv import load_dotenv
import os

from scrapy.exceptions import DropItem

load_dotenv()

table_name = 'ads'
# table_name = 'housing_aggregator_ad'

class AdsDatabasePipeline:

    def __init__(self):
        hostname = os.environ.get('HOSTNAME')
        username = os.environ.get('USERNAME')
        password = os.environ.get('PASSWORD')
        database = os.environ.get('DATABASE')

        self.connection = psycopg2.connect(
                        host=hostname,
                        user=username,
                        password=password,
                        dbname=database,
                        )

        self.cur = self.connection.cursor()

        self.cur.execute(f'''
                         create table if not exists {table_name}(
                                id serial primary key,
                                site varchar(50),
                                category varchar(50),
                                title text,
                                price decimal,
                                currency varchar(5),
                                description text,
                                parse_datetime timestamp,
                                ad_url varchar(400),
                                daily boolean,
                                address text,
                                additional text,
                                images text[],
                                rooms text,
                                apartment_area text,
                                land_area text,
                                series text,
                                furniture text,
                                renovation text,
                                pets text,
                                seller text
                                 )
                         ''')

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        self.cur.execute(f"select * from {table_name} where ad_url = (%s)", (adapter['ad_url'], ))

        if self.cur.fetchone() is not None:
            raise DropItem(f"Duplicate item found: {item!r}")

        if adapter.get('price'):
            price = float(adapter['price'])
        else:
            price = None
        
        self.cur.execute(f'''insert into {table_name}(
                                site,
                                category,
                                title,
                                price,
                                currency,
                                description,
                                parse_datetime,
                                ad_url,
                                daily,
                                address,
                                additional,
                                images,
                                rooms,
                                apartment_area,
                                land_area,
                                series,
                                furniture,
                                renovation,
                                pets,
                                seller
                                 ) values (
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s,
                                         %s
                                         )''', (
                            item['site'],
                            item['category'],
                            item['title'],
                            price,
                            item['currency'],
                            item['description'],
                            item['parse_datetime'],
                            item['ad_url'],
                            item['daily'],
                            item['address'],
                            item['additional'],
                            item['images'],
                            item['rooms'],
                            item['apartment_area'],
                            item['land_area'],
                            item['series'],
                            item['furniture'],
                            item['renovation'],
                            item['pets'],
                            item['seller'],
                            ))

        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

class AdsPipeline:
    pass
