import csv
import json
import math

import scrapy
from scrapy import Request


class HgSpider(scrapy.Spider):
    name = 'hg'
    allowed_domains = ['healthgrade.com']
    start_urls = ['https://www.healthgrades.com']
    custom_settings = {'ROBOTSTXT_OBEY': False, 'LOG_LEVEL': 'INFO',
                       'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
                       'RETRY_TIMES': 5,
                       'FEED_URI': 'dentists.csv',
                       'FEED_FORMAT': 'csv',
                       }

    def start_requests(self):
        with open('..\\healthgrade\\zipcodes.csv', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                url = 'https://www.healthgrades.com/api3/usearch?what=Dentistry&where={}&pageNum={}&sort.provider=bestmatch'.format(
                    row['zip codes'], 1)
                yield Request(url=url, callback=self.parse_pagination)

    def parse_pagination(self, response):
        listing_json = json.loads(response.text)
        total_records = listing_json['search']['searchResults']['totalCount']

        if total_records % 22 == 0:
            total_pages = total_records / 22
        else:
            total_pages = math.ceil(total_records / 22)
        for page in range(1, int(total_pages) + 1):
            start = response.url.split('pageNum')[0]
            end = '&sort' + response.url.split('&sort')[1]
            url = start + 'pageNum={}'.format(page) + end
            yield Request(url, self.parse, dont_filter=True)

    def parse(self, response):
        listing_json = json.loads(response.text)
        results = listing_json['search']['searchResults']['provider']['results']
        for dt in results:
            url = 'https://www.healthgrades.com' + dt['providerUrl']
            yield Request(url, self.parse_detail, dont_filter=True)

    def parse_detail(self, response):
        item = dict()
        detail_json_str = response.text.split("pageState.viewModel = ")[1].split("}());")[0].strip().rstrip(';')
        detail_json = json.loads(detail_json_str)
        item['Url'] = response.url
        item['Dentists Name'] = detail_json['providerDisplayFullName']
        item['Office Name'] = detail_json['officeLocations'][0]['officeLocations'][0]['name']
        item['Address'] = detail_json['officeLocations'][0]['officeLocations'][0]['street']
        item['City'] = detail_json['officeLocations'][0]['officeLocations'][0]['city']
        item['State'] = detail_json['officeLocations'][0]['officeLocations'][0]['state']
        item['Zip'] = detail_json['officeLocations'][0]['officeLocations'][0]['postalCode']
        item['Country'] = detail_json['officeLocations'][0]['officeLocations'][0]['nation']
        item['Age'] = detail_json['age']
        item['Images URLs'] = detail_json['imageUrl']
        item['Phone_number'] = detail_json['officePhone']
        item['Ratings'] = detail_json['displayOverallStarRating']
        item['Website'] = detail_json['websiteUrl']
        item['Biography'] = response.xpath("//p[@data-qa-target='premium-biography']/text()").get()
        item['Medical Degree'] = detail_json['medicalSpecialty']
        item['Practice_Area'] = detail_json['practicingSpecialityName']
        item['insurance_check'] = ','.join(response.xpath("//p[@class='insurance-try-top']/span/a/text()").extract())
        edu_list = []
        educations = detail_json['education']
        for edu in educations:
            edu_and_type = edu['name'] + " :" + edu['type']
            edu_list.append(edu_and_type)
        item['Education'] = ','.join(edu_list)
        item['Fax'] = detail_json['officeLocations'][0]['officeLocations'][0]['fax']
        item['Work Experience Reviews'] = ','.join(
            response.xpath("//div[@class='c-single-comment__comment']/text()").extract())
        timing_json = detail_json['officeLocations'][0]['officeLocations'][0]['officeHours']
        timing_list = []
        for time in timing_json:
            day = time['dayOfWeekName']
            if time['isClosed']:
                close = 'Office_closed'
                time = day + close
            else:
                start_time = time['startTime']
                end_time = time['endTime']
                time = day + start_time + " to " + end_time
            timing_list.append(time)
        item['Timings'] = ';'.join(timing_list)
        yield item
