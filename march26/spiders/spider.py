# -*- coding: utf-8 -*-

from six import u
import re
import scrapy

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

def strip_scheme(url):
    parsed = urlparse(url)
    scheme = "%s://" % parsed.scheme
    return parsed.geturl().replace(scheme, '', 1)

def group(lst, n):
  for i in range(0, len(lst), n):
    val = lst[i:i+n]
    if len(val) == n:
      yield tuple(val)


class March26Spider(scrapy.Spider):
    name = "march26"
    #start_urls = ["https://vk.com/wall-55284725_382633"]
    start_urls = ["https://vk.com/wall-55284725_427126"]

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x26.03.2017) AppleWebKit/26.03.2017 (KHTML, like Gecko) Chrome/26.03.2017 Safari/26.03.2017'
    }

    def is_event_link(self, url):
        return url.startswith('vk.com') or "facebook.com" in url

    def create_request(self, city_name, url):
        if not url:
            return

        if url.startswith('vk.com'):
            full_url = "https://%s" % url
            return scrapy.Request(full_url, callback=self.parse_vk, meta={'city_name': city_name, 'url': url})
        elif "facebook.com" in url:
            full_url = "https://%s" % url
            return scrapy.Request(full_url, callback=self.parse_fb, meta={'city_name': city_name, 'url': url})

    def parse(self, response):
        cities = []

        body = response.css('div.wall_post_text').extract()[0]
        raw_cities = re.findall(u(r'(\<br\>([^\<]+)\s{1}\<a href=[\'"]?([^\'" >]+)[^>]+\>([^\<]+)\<\/a\>( \w (\<a href=[\'"]?[^\'" >]+[^>]+\>([^\<]+)\<\/a\>))?)'), body, re.MULTILINE|re.IGNORECASE|re.UNICODE)
        for (_, city_name, _, link1, _, _, link2) in raw_cities:
            city_name = city_name.replace(u"\u2014", '').strip()
            link1 = strip_scheme(link1)
            link2 = strip_scheme(link2)

            if not self.is_event_link(link1) and not self.is_event_link(link2):
                continue

            # ignore P.S. Text
            if city_name and city_name.startswith("P.S."):
                yield {
                    'type': 'extra',
                    'text': 'Если кого забыли пишите ссылку в <a href="{comment_url}">комменты</a>, добавим. В список берём только встречи (о том, как создать и вести встречу тут: {url})',
                    'url': link1
                }
                continue

            yield {
                'type': 'wall',
                'city_name': city_name,
                'link1': link1,
                'link2': link2,
            }

            req = self.create_request(city_name, link1)
            if req:
                yield req

            req = self.create_request(city_name, link2)
            if req:
                yield req


    def parse_vk(self, response):
        stats = {
            'type': 'stats',
            'city_name': response.meta['city_name'],
            'url': response.meta['url'],
            'counters': {
                'attending': '0',
                'maybe': '0',
                'invited': '0',
            }
        }

        raw_counters = response.css('div.group_counters_wrap a.page_counter')
        for cnt in raw_counters:
            label = cnt.css('div.label::text').extract()[0]
            count = cnt.css('div.count::text').extract()[0]
            if label == 'attending':
                stats['counters']['attending'] = count
            elif label == 'may be':
                stats['counters']['maybe'] = count
            elif label == 'invited':
                stats['counters']['invited'] = count

        yield stats


    def parse_fb(self, response):
        stats = {
            'type': 'stats',
            'city_name': response.meta['city_name'],
            'url': response.meta['url'],
            'counters': {
                'attending': '0',
                'maybe': '0',
                'invited': '0',
            }
        }

        # 18 Going&nbsp;·&nbsp;24 Interested
        body = response.body.decode("utf8")
        raw_counters = re.search(u(r'((\d+)((\.){1}\d+[A-Z]?)?){1} Going&nbsp;\u00B7&nbsp;((\d+)((\.){1}\d+[A-Z]?)?){1} Interested'), body, re.MULTILINE|re.IGNORECASE|re.UNICODE)
        if raw_counters:
            stats['counters']['attending'] = raw_counters.group(1)
            stats['counters']['maybe'] = raw_counters.group(5)

        yield stats

