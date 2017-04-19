# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import datetime

# convert 2.3[K|M] to 2300,2300000
def social_number_to_real_number(n):
    ends = n[-1]
    mul = 1
    if ends == "K":
        mul = 1000
    if ends == "M":
        mul = 1000000
    if mul == 1:
        return int(n)
    else:
        return int(float(n[:-1])*mul)


def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")


class March26Pipeline(object):

    def open_spider(self, spider):
        self.cities_data = {}


    def process_item(self, item, spider):
        city_name = item['city_name']
        if item['type'] == 'wall':
            links = []
            if item['link1']:
                links.append(item['link1'])
            if item['link2']:
                links.append(item['link2'])

            self.cities_data[city_name] = {
                'city_name': city_name,
                'links': links,
                'counters': {},
            }

        if item['type'] == 'stats':
            city = self.cities_data[city_name]
            city['counters'][item['url']] = item['counters']

        return item

    def close_spider(self, spider):
        total_stats = {
            'total_cities': 0,
            'total_cities_without_stats': 0,
            'attending': 0,
            'maybe': 0,
            'invited': 0,
        }

        cities_without_stats = []
        for (city, data) in self.cities_data.items():
            total_stats['total_cities'] += 1

            for sn, c in data['counters'].items():
                num_a = social_number_to_real_number(c['attending'])
                num_i = social_number_to_real_number(c['invited'])
                num_m = social_number_to_real_number(c['maybe'])
                total_stats['attending'] += num_a
                total_stats['maybe'] += num_m
                total_stats['invited'] += num_i

                c['attending'] = num_a
                c['invited'] = num_i
                c['maybe'] = num_m

                if num_a == 0 and num_i == 0 and num_m == 0:
                    total_stats['total_cities_without_stats'] += 1
                    cities_without_stats.append({'city_name': city, 'url': sn})

        stats = {
            'stats_total': total_stats,
            'stats_by_city': self.cities_data,
            'cities_without_stats': cities_without_stats,
            'updated_time': datetime.datetime.utcnow(),
        }

        with open("stats.json", "w") as outfile:
            json.dump(stats, outfile, indent=4, default=datetime_handler)

