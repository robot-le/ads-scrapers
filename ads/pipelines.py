from itemadapter import ItemAdapter
import psycopg2
from dotenv import load_dotenv
import os

from scrapy.exceptions import DropItem

load_dotenv()

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

        self.cur.execute('''
                         create table if not exists ads(
                                 ads_id serial primary key,
                                 site text,
                                 category text,
                                 title text,
                                 price decimal,
                                 currency text,
                                 description text,
                                 parse_datetime timestamp,
                                 ad_url text,
                                 daily boolean,
                                 address text,
                                 additional text,
                                 images text
                                 )
                         ''')

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        self.cur.execute("select * from ads where ad_url = (%s)", (adapter['ad_url'], ))

        if self.cur.fetchone() is not None:
            raise DropItem(f"Duplicate item found: {item!r}")

        if adapter.get('price'):
            price = float(adapter['price'])
        else:
            price = None
        
        self.cur.execute('''insert into ads(
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
                                 images
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
                            ))

        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

class AdsPipeline:
    pass
