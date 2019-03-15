
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


PROXY = '109.237.147.213:37581'
POSTCODE = "WC2N 5DU"


class ExtractItem(scrapy.Item):
    Make = scrapy.Field()
    Model = scrapy.Field()
    Condition = scrapy.Field()
    Price = scrapy.Field()
    Year = scrapy.Field()
    Body_Type = scrapy.Field()
    Fuel_Type = scrapy.Field()
    Enginer_Size = scrapy.Field()
    Fuel_Consumption = scrapy.Field()
    Acceleration = scrapy.Field()
    GearBox = scrapy.Field()
    DriveTrain = scrapy.Field()
    CO2_Emissions = scrapy.Field()
    Doors = scrapy.Field()
    Seats = scrapy.Field()
    Annual_Tax = scrapy.Field()
    Ad_Link = scrapy.Field()


class AutoTraderSpider(scrapy.Spider):
    name = "autotrader_cars_spider"
    total_urls = 1

    def start_requests(self, ):
        base_url = "https://www.autotrader.co.uk/json/search/options?"\
              "advertising-location=at_cars&"\
              f"postcode={POSTCODE.lower().replace(' ', '')}&"\
              f"price-search-type=total-price"
        response = requests.get(base_url)
        json_response = response.json()
        for make in json_response['options']['make']:
            make_url = base_url + f"&make={make['uriValue']}"
            yield scrapy.Request(
                url=make_url,
                callback=self.process_each_make,
                meta={
                    'make': make['uriValue']
                }
            )

        # url = "https://www.autotrader.co.uk/car-search?advertising-location=at_cars&search-target=usedcars&is-quick-search=TRUE&radius=&make=LAND ROVER&model=DISCOVERY SPORT&price-search-type=total-price&price-from=20000&price-to=22500&postcode=wc2n5du"
        # yield scrapy.Request(
        #     url=url,
        #     callback=self.parse_results,
        #     meta={'page_number': 2}
        # )

    def process_each_make(self, response):
        base_url = response.url
        json_response = json.loads(response.text)
        for model in json_response['options']['model']:
            model_url = base_url + f"&model={model['uriValue']}"
            yield scrapy.Request(
                url=model_url,
                callback=self.process_each_model,
                meta={
                    'make': response.meta['make'],
                    'model': model['uriValue']
                }
            )

    def process_each_model(self, response):
        make = response.meta['make']
        model = response.meta['model']
        json_response = json.loads(response.text)
        if json_response['resultCount'] == "0":
            return
        price_from = [
            price['uriValue'] for price in
            json_response['options']['price-from']
        ]
        price_to = [
            price['uriValue'] for price in
            json_response['options'].get('price-to', {})
            if price
        ]
        price_searches = list()
        for pp in price_from:
            try:
                if price_to:
                    price_searches.append(
                        (int(pp), min([int(x) for x in price_to
                         if int(x) > int(pp)]))
                    )
                else:
                    price_searches.append((int(pp), ""))
            except:
                pass
        for price_search in price_searches:
            results_url = "https://www.autotrader.co.uk/car-search?"\
                  "advertising-location=at_cars&"\
                  "search-target=usedcars&"\
                  "is-quick-search=TRUE&"\
                  "radius=&"\
                  f"make={make}&"\
                  f"model={model}&"\
                  "price-search-type=total-price&"\
                  f"price-from={price_search[0]}&"\
                  f"price-to={price_search[1]}&"\
                  f"postcode={POSTCODE.lower().replace(' ', '')}"
            self.total_urls += 1
            yield scrapy.Request(
                url=results_url,
                callback=self.parse_results,
                meta={'page_number': 2}
            )

    def parse_results(self, response):
        page_number = response.meta['page_number']
        ad_ids = response.xpath(
            '//li[@class="search-page__result"]/@id').extract()
        for _id in ad_ids:
            item_url = f"https://www.autotrader.co.uk/json/fpa/initial/{_id}"
            yield scrapy.Request(
                url=item_url,
                callback=self.parse_item_detail,
                meta={'_id': _id}
            )
        next_page = response.xpath('//a[@class="pagination--right__active"]')
        if next_page:
            next_page_url = response.url.split('&page=')[0] +\
                f'&page={page_number}'
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_results,
                dont_filter=True,
                meta={'page_number': page_number + 1}
            )

    def parse_item_detail(self, response):
        json_response = json.loads(response.text)
        item = ExtractItem()
        item['Make'] = json_response['vehicle']['make']
        item['Model'] = json_response['vehicle']['model']
        item['Condition'] = json_response['vehicle']['condition']
        item['Price'] = json_response['pageData']['tracking']['vehicle_price']
        item['Year'] = json_response['vehicle'].get('year') or\
            json_response['pageData']['tracking'].get('vehicle_year')
        item['Body_Type'] = json_response['vehicle']['keyFacts']['body-type']
        item['Fuel_Type'] = json_response['vehicle']['keyFacts']['fuel-type']
        item['Enginer_Size'] = json_response[
            'vehicle']['keyFacts'].get('engine-size') or\
            json_response['pageData']['tracking'].get('engine_size')
        item['Fuel_Consumption'] = json_response[
            'pageData']['tracking'].get('average_mpg')
        item['Acceleration'] = json_response[
            'pageData']['tracking'].get('acceleration')
        item['GearBox'] = json_response[
            'pageData']['tracking'].get('gearbox')
        item['DriveTrain'] = json_response[
            'pageData']['tracking'].get('drivetrain')
        item['CO2_Emissions'] = json_response[
            'pageData']['tracking'].get('co2_emissions')
        item['Doors'] = json_response['vehicle']['keyFacts'].get('doors')
        item['Seats'] = json_response['vehicle']['keyFacts'].get('seats')
        item['Annual_Tax'] = json_response[
            'pageData']['tracking'].get('annual_tax')
        item['Ad_Link'] = json_response['pageData'].get('canonical') or\
            "https://www.autotrader.co.uk/classified/"\
            f"advert/{response.meta['_id']}"
        derivative_id = json_response['vehicle'].get('derivativeId')
        if derivative_id:
            derivative_url =\
                "https://www.autotrader.co.uk/json/taxonomy/"\
                f"technical-specification?derivative={derivative_id}"
            yield scrapy.Request(
                url=derivative_url,
                callback=self.parse_derivative,
                dont_filter=True,
                meta={'item': item}
            )
        else:
            yield item

    def parse_derivative(self, response):
        item = response.meta['item']
        json_response = json.loads(response.text)
        for tech_spec in json_response['techSpecs']:
            if not tech_spec['specName'] == "Economy & performance":
                continue
            for spec in tech_spec['specs']:
                if spec['name'] == "Fuel consumption (combined)":
                    item['Fuel_Consumption'] = spec['value']
                if ' seconds' in spec['value'] and\
                   not item.get('Acceleration'):
                    item['Acceleration'] = spec['value']
                if ' emissions' in spec['value'] and\
                   not item.get('CO2_Emissions'):
                    item['CO2_Emissions'] = spec['value']
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
            'pipelines_cars.ExtractPipeline': 300,
        },
        'DOWNLOAD_DELAY': request_delay,
        'DOWNLOAD_TIMEOUT': timeout,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines_cars.BanPolicy',
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
