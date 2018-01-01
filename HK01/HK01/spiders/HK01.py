from urllib.parse import unquote
from datetime import datetime

import scrapy


ARTICAL_URL = 'https://www.hk01.com/article/{}'


class Hk01Spider(scrapy.Spider):
    name = 'HK01'

    @staticmethod
    def _get_artical_url(artical_id):
        return ARTICAL_URL.format(artical_id)

    @staticmethod
    def _clean_tag_ids(tag_ids):
        return [int(id.split('/')[-1])
                for id in tag_ids if 'javascript' not in id]

    @staticmethod
    def _clean_ts(ts):
        if '最後更新日期：' in ts:
            ts = ts.replace('最後更新日期：', '')
        if '發佈日期：' in ts:
            ts = ts.replace('發佈日期：', '')
        return ts

    @staticmethod
    def _get_source(paragraph):
        src = paragraph[-2]
        if '（' in src or '）' in src:
            src = src[1:-1].split('、')
        else:
            src = ''
        return src

    @staticmethod
    def _zip_tags(tag_ids, tag_names):
        cleaned_tag_ids = [int(id.split('/')[-1])
                           for id in tag_ids if 'javascript' not in id]
        return list(zip(cleaned_tag_ids, tag_names))

    def start_requests(self):
        for artical_id in range(100000, 150000):
            url = ARTICAL_URL.format(artical_id)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            artical_category = response.url.split('/')[3],
        except:
            artical_category = []

        try:
            artical_id = int(response.url.split('/')[4]),
        except:
            artical_id = []

        try:
            title = response.css('div.article_tit h1::text').extract_first(),
        except:
            title = []

        try:
            channel = response.css('div.channel_tit tit::text').extract_first(),
        except:
            channel = []

        try:
            editors = []
            for editor in response.css('div.editor::text').extract():
                editors.append(editor.replace('撰文：', '').replace('\t', '').replace('\n', ''))
        except:
            editor = []

        try:
            release_ts = self._clean_ts(response.css('div.date::text').extract()
                                        [0].replace('\t', '').replace('\n', '')),
        except:
            release_ts = []

        try:
            last_updated_ts = self._clean_ts(response.css('div.date::text').extract()
                                             [1].replace('\t', '').replace('\n', '')),
        except:
            last_updated_ts = []

        try:
            abstract = response.css('li.article_summary_pt h2::text').extract_first().replace(u'\u3000', u''),
        except:
            abstract = []

        try:
            paragraph = '\n'.join(response.selector.xpath(
                '/html/body/section/div[2]/div[1]/div[1]/section[2]').xpath('.//p/text()').extract()[:-1]),
        except:
            paragraph = []

        try:
            tag_ids = self._clean_tag_ids(response.css('div.tag_txt a[href]::attr(href)').extract()),
        except:
            tag_ids = []

        try:
            tag_names = response.css('div.tag_txt h4::text').extract(),
        except:
            tag_names = []

        try:
            tags = self._zip_tags(response.css('div.tag_txt a[href]::attr(href)').extract(
            ), response.css('div.tag_txt h4::text').extract()),
        except:
            tags = []

        item = {
            'url': response.url,
            'artical_category': unquote(artical_category[0]) if artical_category is not None else "",
            'artical_id': artical_id[0] if artical_id is not None else 0,
            'title': title[0] if title is not None else "",
            'channel': channel[0] if channel is not None else "",
            'editors': editors if editors is not None else "",
            'release_ts': datetime.strptime(release_ts[0], '%Y-%m-%d %H:%M') if release_ts is not None else datetime.min,
            'last_updated_ts': datetime.strptime(last_updated_ts[0], '%Y-%m-%d %H:%M') if last_updated_ts is not None else datetime.min,
            'abstract': abstract if abstract is not None else "",
            'paragraph': paragraph[0] if paragraph is not None else "",
            'tag_ids': tag_ids[0] if tag_ids is not None else [],
            'tag_names': tag_names[0] if tag_names is not None else "",
            'tags': tags[0] if tags is not None else [],
            'spider_ts': datetime.now(),
            # 'source': self._get_source(response.selector.xpath('/html/body/section/div[2]/div[1]/div[1]/section[2]').xpath('.//p/text()').extract())
        }
        yield item
