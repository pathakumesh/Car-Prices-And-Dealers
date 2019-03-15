
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


PROXY = '216.215.123.174:8080'


def get_coordinates():
    coordinates = list()
    for row in csv.DictReader(
      open('uk_postcodes.csv', 'r'), ["id", "postcode", "lat", "lng"]):
        coordinates.append((row['lat'], row['lng']))
    return coordinates


class ExtractItem(scrapy.Item):
    Name = scrapy.Field()
    Address = scrapy.Field()
    City_zip = scrapy.Field()
    Telephone = scrapy.Field()
    Reviews = scrapy.Field()
    Stars = scrapy.Field()
    Page_Link = scrapy.Field()


class CarGurusSpider(scrapy.Spider):
    name = "cargurus_dealers_spider"
    base_url = "https://www.cargurus.co.uk/Cars/mobile/"\
               "listDealersNearMe.action?latitude={}&longitude={}"
    scraped_items = list()

    def start_requests(self, ):
        for coordinate in get_coordinates():
            result_url = self.base_url.format(coordinate[0], coordinate[1])
            yield scrapy.Request(
                url=result_url,
                callback=self.parse_results,
            )

    def parse_results(self, response):
        results = response.xpath('//a[@class="cg-menu-list-item"]')
        for result in results:
            item = ExtractItem()
            link = result.xpath('@href').extract_first()
            if link in self.scraped_items:
                continue
            if link:
                item['Page_Link'] = "https://www.cargurus.co.uk" + link
                self.scraped_items.append(link)
            item['Name'] = result.xpath('h3/span[1]/text()').extract_first()

            rating = result.xpath(
                'div//div[@class="ratingValue"]/'
                'i[1]/@title').re(r'(.*) out of')
            if rating:
                item['Stars'] = rating[0]

            reviews = result.xpath(
                'div//div[@class="cg-spInfo-ratingReviews text-right"]/'
                'text()').re(r'(.*) reviews')
            if reviews:
                item['Reviews'] = reviews[0]

            address = result.xpath(
                'div[@class="cg-spInfo-address pull-left"]/text()').extract()
            item['Address'] = address[0].strip()\
                .replace('\n', '').replace('\r', '')
            item['City_zip'] = address[1].strip()
            item['Telephone'] = result.xpath(
                'p[@class="cg-spInfo-phone"]/text()').extract_first()
            yield item

        next_page_url = response.xpath(
            '//a[@span[@class="glyphicon glyphicon-chevron-right"]]')
        if next_page_url:
            next_page_url = "https://www.cargurus.co.uk" + next_page_url
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_results,
            )


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
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines_dealers.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(CarGurusSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    timeout = 30
    run_spider(no_of_threads, request_delay, timeout)
