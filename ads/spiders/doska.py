import scrapy
from ads.items import HousingItems
from datetime import datetime, timedelta


class DoskaSpider(scrapy.Spider):
    name = 'doska'
    allowed_domains = ['doska.kg']
    start_urls = ['https://doska.kg/cat:117/&image=1&region=1&type=5']

    def parse(self, response):
        ad_elements = response.xpath('//div[@class="doska_last_items_list"]/*[not(contains(@class, "premium-block"))]')

        for ad_el in ad_elements:
            category = ad_el.xpath('./div[@class="list_full_title"]/div/text()').get().strip()
            upload_time = ad_el.xpath('.//div[@class="list_full_date"]/text()').get().strip()
            url = 'https://doska.kg/' + ad_el.xpath('./div[@class="list_full_title"]/a/@href').get()
            yield scrapy.Request(
                url=url,
                callback=self.parse_ad,
                meta={
                    'category': category,
                    'upload_time': upload_time,
                }
            )

        next_page = response.xpath('//div[@class="elem"]').xpath('./a[contains(text(), "след")]/@href').get()
        if next_page is not None:
            next_url = 'https://doska.kg/' + next_page
            yield scrapy.Request(url=next_url, callback=self.parse)

    def parse_ad(self, response):
        items = HousingItems()
        div_in_col_el = response.xpath('//div[@itemtype="http://schema.org/Place"]/../div')
        items_list = [x for x in response.xpath('//div[@itemtype="http://schema.org/Place"]/div')] + div_in_col_el[1:3]
        items_dict = {key.xpath('.//b/text()').get().strip(): value.xpath('.//text()').getall()[-1].strip() for (key, value) in dict(zip(items_list, items_list)).items()}  # TODO: графа этаж не парсится

        items['site'] = 'https://doska.kg/'
        items['currency'] = response.xpath('//span[@itemprop="priceCurrency"]/@content').get()
        items['price'] = response.xpath('//span[@itemprop="price"]/@content').get()
        items['description'] = ''.join(response.xpath('//div[@itemtype="http://schema.org/Place"]/../../div[2]/span/text()').getall())
        items['title'] = response.xpath('//h1[@class="item_title"]/text()').getall()[-1].strip()
        items['category'] = response.meta.get('category')
        items['parse_datetime'] = datetime.now()
        items['ad_url'] = response.url

        months = {
            ' Января ':   '.01.',
            ' Февраля ':  '.02.',
            ' Марта ':    '.03.',
            ' Апреля ':   '.04.',
            ' Мая ':      '.05.',
            ' Июня ':     '.06.',
            ' Июля ':     '.07.',
            ' Августа ':  '.08.',
            ' Сентября ': '.09.',
            ' Октября ':  '.10.',
            ' Ноября ':   '.11.',
            ' Декабря ':  '.12.',
        }

        upload_time = response.meta.get('upload_time')

        if 'Позавчера' in upload_time:
            date = datetime.now() - timedelta(days=2)
            items_dict['upload_time'] = date.strftime('%d.%m.%Y')
        elif 'Вчера' in upload_time:
            date = datetime.now() - timedelta(days=1)
            items_dict['upload_time'] = date.strftime('%d.%m.%Y')
        elif 'Сегодня' in upload_time:
            items_dict['upload_time'] = datetime.now().strftime('%d.%m.%Y')
        else:
            for key, value in months.items():
                items_dict['upload_time'] = upload_time.replace(key, value)

        yield items
