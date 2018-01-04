from datetime import datetime
from dateutil import tz
import re

HKT = tz.gettz('Asia/Hong_Kong')
UTC = tz.gettz('UTC')

def extract_abstract(abstract_css):
    try:
        abstract = abstract_css.replace(u'\u3000', u'')
    except:
        abstract = ""
    return abstract

def extract_editors(src_editors):
    try:
        editors = []
        for editor in src_editors:
            editors.append(editor.replace('撰文：', '').replace('\t', '').replace('\n', ''))
    except:
        editors = []
    return editors

def extract_paragraph(paragraphs):
    try:
        paragraph = '\n'.join(paragraphs)
    except:
        paragraph = []
    return paragraph

def extract_sources(paragraph):
    for src in paragraph[-5:]:
        if re.match(r"^\（(.*?)\）$", src):
            source = re.match(r"^\（(.*?)\）$", src).group(1)
    sources = []
    try:
        if '/' in source: 
            sources = source.split('/')
        if '、' in source:
            sources = source.split('、')
        if '，' in source:
            sources = source.split('，')
    except:
        pass
    return sources

def extract_tag_ids(tag_ids):
    dedup_tag_ids = set(tag_ids)
    dedup_tag_ids.remove("javascript:void(0)")
    ids = []
    for id in dedup_tag_ids:
        ids.append(int(id.split('/')[-1]))
    return ids

def extract_tag_names(tag_names_css):
    tag_names = []
    try:
        tag_names = response.css('div.tag_txt h4::text').extract()
    except:
        tag_names = []
    return tag_names


def extract_title(title_css):
    # if channel in ['01%E5%BD%B1%E5%83%8F']:
    try:
        title = title_css.replace(u'\u3000', u'')
    except:
        title = ""
    return title

def extract_release_ts(ts):
    try:
        release_ts = ts.replace('\t', '').replace('\n', '').replace('發佈日期：', '')
    except:
        release_ts = 0
    ts = datetime.strptime(release_ts, '%Y-%m-%d %H:%M').replace(tzinfo=HKT).astimezone(UTC).timestamp()
    
    return int(ts) 

def extract_last_update_ts(ts):
    try:
        last_update_ts = ts.replace('\t', '').replace('\n', '').replace('最後更新日期：', '')
    except:
        last_update_ts = 0
    ts = datetime.strptime(last_update_ts, '%Y-%m-%d %H:%M').replace(tzinfo=HKT).astimezone(UTC).timestamp()
    return int(ts)

def zip_tags(tag_ids, tag_names):
    return dict(zip(tag_ids, tag_names))

def ts_to_timestr(ts):
    return datetime.fromtimestamp(int(ts)).replace(tzinfo=UTC).astimezone(HKT).strftime('%Y-%m-%d')