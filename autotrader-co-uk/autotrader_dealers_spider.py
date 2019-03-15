import time
import csv
import json
import re
import scrapy
import hashlib
import requests
from math import ceil
from random import randint
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess


PROXY = '83.149.70.159:13012'
POSTCODE = "WC2N 5DU"


class ExtractItem(scrapy.Item):
    Name = scrapy.Field()
    Reviews = scrapy.Field()
    Stars = scrapy.Field()
    Address = scrapy.Field()
    Cars_Listed = scrapy.Field()
    Page_Link = scrapy.Field()
    Ph_no_1 = scrapy.Field()
    Ph_no_2 = scrapy.Field()


class AutoTraderSpider(scrapy.Spider):
    name = "autotrader_dealers_spider"
    total_urls = 1
    base_url = "https://www.autotrader.co.uk/ajax/dealer-search/"\
               "dealer-search-option?advertising-location=at_cars&"\
               f"postcode={POSTCODE.lower().replace(' ', '')}&"\
               "radius=1500&forSale=on&toOrder=on&"\
               "&sort=with-retailer-reviews"

    def start_requests(self, ):
        url = self.base_url + "&page=1"
        response = requests.get(url)
        json_response = response.json()
        html_response = fromstring(json_response['fields'][0]['html'])
        for make in html_response.xpath('//option[not(text()="Any")]/text()'):
            result_url = self.base_url + f"&make={make}&page=1"
            yield scrapy.Request(
                url=result_url,
                callback=self.parse_results,
                meta={
                    'make': make
                }
            )

    def parse_results(self, response):
        make = response.meta['make']
        try:
            json_response = json.loads(response.text)
            html_response = fromstring(json_response['html'])
        except:
            match = re.findall(
                r'.*?(\<script.*\<\/nav\>)',
                response.text,
                re.DOTALL
            )
            if not match:
                return
            html_response = fromstring(match[0])
        results = html_response.xpath('//article[@class="dealerList__item"]')
        for result in results:
            name = result.xpath('header/a/span/text()')
            if not name:
                continue
            item = ExtractItem()
            item['Name'] = name[0].strip()

            reviews = result.xpath(
                'header//meta[@itemprop="ratingValue"]/@content')
            item['Stars'] = reviews[0].strip() if reviews else None

            stars = result.xpath(
                'header//meta[@itemprop="ratingCount"]/@content')
            item['Reviews'] = stars[0].strip() if stars else None

            address = result.xpath(
                'a//p[@class="dealerList__itemAddress"]/text()')
            if address:
                address = address[0].strip().replace('\n', '')
                address = re.sub(r'\s{2,}', ' ', address)
                item['Address'] = address

            cars_listed = result.xpath(
                'a//span[@class="dealerList__itemCountNumber"]/text()')
            item['Cars_Listed'] = cars_listed[0].strip()\
                if cars_listed else None

            page_link = result.xpath('a/@href')
            if page_link:
                url = page_link[0]
                item['Page_Link'] = "https://www.autotrader.co.uk" + url

                dealer_id = re.findall(r'.*-(\d+)', url)

                if dealer_id:
                    dealer_url = 'https://www.autotrader.co.uk/json/seo/'\
                                 f'dealer?dealerId={dealer_id[0]}&channel=cars'

                    yield scrapy.Request(
                        url=dealer_url,
                        callback=self.parse_dealer_info,
                        meta={'item': item}
                    )
                else:
                    yield item
            else:
                yield item

        next_page_index = html_response.xpath(
            '//li[contains(@class,"pagination--li") and not(a)]/'
            'following-sibling::li[1]/a/text()')
        if next_page_index:
            result_url = self.base_url +\
                f"&make={make}&page={next_page_index[0]}"
            yield scrapy.Request(
                url=result_url,
                callback=self.parse_results,
                meta={
                    'make': make
                }
            )

    def parse_dealer_info(self, response):
        item = response.meta['item']
        json_response = json.loads(response.text)
        telephone1 = json_response['dealer'].get('telephone1')
        telephone2 = json_response['dealer'].get('telephone2')
        if telephone1:
            item["Ph_no_1"] = telephone1
        if telephone2:
            item["Ph_no_2"] = telephone2
        yield item


def run_spider(no_of_threads, request_delay, timeout):
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'ITEM_PIPELINES': {
            'pipelines_dealers.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'DOWNLOAD_TIMEOUT': timeout,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 404, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines_dealers.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(AutoTraderSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    timeout = 30
    run_spider(no_of_threads, request_delay, timeout)
