
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


def obtain_models():
    url = "https://www.cargurus.co.uk/Cars/"\
           "getCarPickerReferenceDataAJAX.action?"\
           "forPriceAnalysis=false&showInactive=false"\
           "&newCarsOnly=false&useInventoryService=true&"\
           "quotableCarsOnly=false&carsWithRegressionOnly=false"\
           "&localeCountryCarsOnly=true"
    response = requests.get(url)
    json_response = response.json()
    car_models = dict()
    for make, model in json_response['allMakerModels'].items():
        car_models[make] = dict()
        car_models[make].update({
            popular['modelId']: popular['modelName']
            for popular in model.get('popular', [])
        })
        car_models[make].update({
            unpopular['modelId']: unpopular['modelName']
            for unpopular in model.get('unpopular', [])
        })
    return car_models


class ExtractItem(scrapy.Item):
    Make = scrapy.Field()
    Model = scrapy.Field()
    Year = scrapy.Field()
    Count = scrapy.Field()
    Page_Link = scrapy.Field()


class CarGurusSpider(scrapy.Spider):
    name = "cargurus_cars_spider"
    total_urls = 1
    start_urls = ["https://www.cargurus.co.uk/"]

    def parse(self, response):
        car_models = obtain_models()
        self.cars = dict()
        for make in response.xpath('//optgroup[@label="All makes"]/option'):
            make_id = make.xpath('@value').extract_first()
            make_name = make.xpath('text()').extract_first()
            self.cars.update({
                f'{make_id}_{make_name}': car_models[make_id]
            })
        # print(self.cars)
        # self.cars = {
        #   'm265_Abarth': {
        #     'd3009': '500',
        #     'd4189': '595',
        #     'd4190': '124',
        #     'd3010': '500C',
        #     'd4191': '595C',
        #     'd4245': '695'
        #   },
        #   'm294_AC': {
        #     'd3301': 'Cobra'
        #   },
        #   'm256_BMW': {
        #     'd2889': '1 Series',
        #     'd2872': '2 Series',
        #     'd2887': '3 Series',
        #     'd2879': '4 Series',
        #     'd2888': '5 Series',
        #     'd2884': '6 Series',
        #     'd2885': '7 Series',
        #     'd2878': 'i3',
        #     'd2890': 'X1',
        #     'd4930': 'X2',
        #     'd2877': 'X3',
        #     'd2886': 'X4',
        #     'd2882': 'X5',
        #     'd2891': 'X6',
        #     'd2880': 'Z4',
        #     'd2881': '8 Series',
        #     'd2873': 'i8',
        #     'd2883': 'Z1',
        #     'd2875': 'Z3'
        #   },

        # }
        for car_make, car_model in self.cars.items():
            for model_id, model_name in car_model.items():
                url = "https://www.cargurus.co.uk/Cars/getCarList.action?"\
                      f"showInactive=false&model={model_id}&newCarsOnly=false"\
                      "&quotableOnly=undefined&useInventoryService=true"\
                      "&carsWithRegressionOnly=false&localeCountryOnly=true"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_years,
                    meta={
                        'make': car_make,
                        'model_id': model_id,
                        'model_name': model_name,
                    }
                )

    def parse_years(self, response):
        years = re.findall(r'A=(\w+)=(\d+)', response.text)
        years.reverse()
        for index, year in enumerate(years[:-1]):
            start_year = year
            end_year = years[index+1]

            result_url = "https://www.cargurus.co.uk/Cars/inventorylisting/"\
                         "ajaxFetchSubsetInventoryListing.action"\
                         "?sourceContext=carGurusHomePageModel"
            form_data = {
                "zip": POSTCODE,
                "address": "London, Greater London, GB",
                "latitude": "51.507198333740234",
                "longitude": "-0.1282690018415451",
                "distance": "660",
            }
            form_data.update({
                "selectedEntity": start_year[0],
                "entitySelectingHelper.selectedEntity2": end_year[0],
                "page": "1",
                "startYear": start_year[1],
                "endYear": end_year[1],
            })
            ui_url = "https://www.cargurus.co.uk/Cars/inventorylisting/"\
                     "viewDetailsFilterViewInventoryListing.action?"\
                     "sourceContext=carGurusHomePageModel&"\
                     "newSearchFromOverviewPage=true&"\
                     "inventorySearchWidgetType=AUTO&"\
                     f"entitySelectingHelper.selectedEntity={start_year[0]}&"\
                     f"entitySelectingHelper.selectedEntity2={end_year[0]}&"\
                     "zip=WC2N+5DU&distance=660&searchChanged=true&"\
                     "modelChanged=false&filtersModified=true"
            yield scrapy.FormRequest(
                url=result_url,
                callback=self.parse_results,
                formdata=form_data,
                meta={
                    'make': response.meta['make'],
                    'model_id': response.meta['model_id'],
                    'model_name': response.meta['model_name'],
                    'ui_url': ui_url
                }
            )

    def parse_results(self, response):
        json_response = json.loads(response.text)
        year_map = dict()
        for result in json_response['listings']:
            if 'carYear' in result:
                year_map[result['carYear']] =\
                    year_map.get(result['carYear'], 0) + 1
        for year, count in year_map.items():
            item = ExtractItem()
            item['Make'] = response.meta['make'].split('_')[-1]
            item['Model'] = response.meta['model_name']
            item['Year'] = year
            item['Count'] = count
            item['Page_Link'] = response.meta['ui_url']
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
    process.crawl(CarGurusSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 10
    request_delay = 0.2
    timeout = 30
    run_spider(no_of_threads, request_delay, timeout)
