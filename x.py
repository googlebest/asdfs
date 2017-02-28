# -*- coding: utf-8 -*-
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.log import *
from crawler_bhinneka.settings import *
from crawler_bhinneka.items import *
import pprint
from MySQLdb import escape_string
import urlparse
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
'''
Local VARIABLE
'''
cursor = CONN.cursor()
pp = pprint.PrettyPrinter(indent=4)
phonebook = {}
update_thread = 0
list_thread= []
def add_thread_sql(thrad_id, thread_title,last_page):


    if select_thread_id(thrad_id):
        return False
    sql = "INSERT INTO thread (id, thread_title, last_page, last_page_current,diable, create_time,update_time) \
    values( %s,'%s',%s,0,0,NOW(),NOW()" % (
    thrad_id, escape_string(thread_title.decode('utf8')), last_page,0)
    print(sql)
    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

    #cursor2.execute(sql)
    return True
def select_thread_id(thread_id):
    sql = "SELECT id FROM thread WHERE id = %s" % (thread_id)
   # print sql
    if cursor.execute(sql):

        row = cursor.fetchone()
        print(row)
        if row is not None:

            return row[0]
        else :
            return None
def complete_url(string):
    """Return complete url"""
    return "http://www.bhinneka.com" + string


def get_base_url(url):
    """
    >>> urlparse.urlparse('http://tmcblog.com')
    >>> ParseResult(scheme='http', netloc='tmcblog.com',
        path='', params='', query='', fragment='')
    """
    if url != "":
        u = urlparse.urlparse(url)
        return "%s://%s" % (u.scheme, u.netloc)
    else:
        return ""


def encode(str):
    return str.encode('utf8', 'ignore')


def insert_table(datas):
    """
    Just MySQL Insert function
    """
    sql = "INSERT INTO %s (name, link, categories, price, time_capt) \
values('%s', '%s', '%s', '%s', NOW())" % (SQL_TABLE,
    escape_string(datas['item_name']),
    escape_string(datas['item_link']),
    escape_string(datas['item_category']),
    escape_string(datas['item_price'])
    )
    # print sql
    if cursor.execute(sql):
        return True
    else:
        print "Something wrong5"

def select_current_thread():
    current_id = select_counter_current();
    sql = "SELECT thread_id,last_page,last_page_current FROM thread WHERE last_page_current < last_page ORDER BY last_page ASC limit 1"
    if cursor.execute(sql):
        print("chay qua")

        row = cursor.fetchone()
        print(row)
        if row is not None:

            return row
        else :
            return None

def update_counter_current(thread_id):
    sql = "UPDATE counter_current SET id=%s WHERE id > -1" %(thread_id)

    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

def select_counter_current():
    sql = "SELECT id FROM counter_current"
    if cursor.execute(sql):

        row = cursor.fetchone()
        #print(row)
        if row is not None:

            return row[0]
        else :
            return None
def NextURL():
    thread_property = select_current_thread()

    list_of_urls = []
    if thread_property is None:
        update_counter_current(1)
    phonebook[thread_property[0]] = thread_property[1]
    print(phonebook)
    print(thread_property[1] > thread_property[2])
    if thread_property[1] > thread_property[2]:
        url_thread = ["http://ebassist.com/forum/archive/index.php/t-"+str(thread_property[0])+"-p-%d.html/" % i for i in xrange(thread_property[2],thread_property[1]+1)]
        print(url_thread)
        list_of_urls += url_thread
        if len(list_thread) > 100:
            exit()
        for next_url in list_of_urls:
            yield next_url
#    url_pattern = "http://vozforums.com/printthread.php?t=1346120&pp=20&page=3"
   # select_current_thread
    else:
        print("not need running")
        # url_thread = "http://vozforums.com/printthread.php?t="+str(thread_property[0])+"&pp=10&page=%d/" % thread_property[2] 
        # print(url_thread)
        update_counter_current(thread_property[0])
       
        yield url_thread


def insert_redis(command, key1, key2):
    """
    Just Redis Insert function
    """
    if command == 'sadd':
        if r.sadd(key1, key2):
            return True
    if command == 'set':
        if r.set(key1, key2):
            return True
def update_last_crrent_page(last_page_current, thread_id):
    sql = "UPDATE thread SET last_page_current=%s WHERE id = %s" %(last_page_current, thread_id)
    print(sql)
    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
def update_lastpage(lastofpage, thread_id):
    sql = "UPDATE thread SET last_page=%s WHERE id = %s" %(lastofpage, thread_id)
    print(sql)
    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
def update_diable(thread_id):
    sql = "UPDATE thread SET diable=%s WHERE id = %s" %(1, thread_id)
    print(sql)
    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
def inser_thread_content(thread_id, thread_page, thread_content):
    sql = "INSERT IGNORE INTO thread_part (thread_id, thread_page, content) \
    values( %s,%s,'%s' )" % (
    thread_id, thread_page, escape_string(thread_content))
    #print(sql)
    # print sql
    try:
        cursor.execute(sql)
        CONN.commit()

    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
def encode(str):
    return str.encode('utf8', 'ignore')
class BhinnekaSpider(CrawlSpider):
    """
    1. Goto http://www.bhinneka.com/categories.aspx
    2. Find some interesting link
    (http://www.bhinneka.com/aspx/products/pro_display_products.aspx?\
    CategoryID=0115)
    3. Save Our data
    """
    allowed_domains = []
    name = 'voz'
    start_urls = []
    url = NextURL()
    def start_requests(self):
        """
        NOTE: This method is ONLY CALLED ONCE by Scrapy (to kick things off).
        Get the first url to crawl and return a Request object
        This will be parsed to self.parse which will continue
        the process of parsing all the other generated URLs
        """

        ## grab the first URL to being crawling
        start_url = self.url.next()

        #log.msg('START_REQUESTS : start_url = %s' % start_url)

        request = Request(start_url, dont_filter=True)

        ### important to yield, not return (not sure why return doesn't work here)
        yield request
    def parse(self, response):
        """
        Parse List Category page, example :
        http://www.bhinneka.com/categories.aspx
        """
        thread_id_c = response.request.url.split('t-')
        thread_id_s = thread_id_c[1].split('-p')[0]
        print(response.request.url)

        page_number_c = response.request.url.split('-p-')
        page_number_s = thread_id_c[1].split('.html')[0]
        # hxs = HtmlXPathSelector(response)
        lastpage = response.xpath('//div[@id="pagenumbers"]//a').extract()
        contents = response.xpath('//div[@class="posttext"]').extract()
        usernames = response.xpath('//div[@class="username"]//text()').extract()



        # HXS to find url that goes to detail page
        # items = hxs.xpath('//td[@class="page"]');
        content = ""
        # for item in items:
        #     content += item.extract()
        #page sel.xpath('//td[@class="vbmenu_control"][1]').extract()
        # print(hxs.xpath('//td[@class="vbmenu_control"][1]/text()').extract())
        if len(lastpage) == 0:
            update_diable(thread_id_s)

            # update_counter_current(thread_id_s)
            exit()
        # pages = hxs.xpath('//td[@class="vbmenu_control"][1]/text()').extract()[0]

        page = pages.split(' ');
        print page[3];
        print page[1];

        update_last_crrent_page(page[1],thread_id_s)
        if update_thread == 1:
            exit()
        inser_thread_content(thread_id_s, page[1], content)
        update_counter_current(thread_id_s)
        if thread_id_s not in list_thread :
            print("tuyenxnguyen")
            if long(thread_id_s) in phonebook :
                 if phonebook[long(thread_id_s)] < long(page[3]):
                    update_lastpage(page[3], thread_id_s)
            list_thread.append(thread_id_s)
        #list_thread.append()
        #print items
        next_url = self.url.next()
        yield Request(next_url)
     #  sel.xpath('//*[contains(@id,"thread_title")]/@id').extract()[25]
      #  u'threadtitle_4167489' post_message_
      #   sel.xpath('//*[contains(@id,"post_message_")]/@id').extract()
      #   thread name sel.xpath('//td[@class="alt1"]/div/a[@id="thread_title_4187604"]/text()').extract()
       #    //td[@id="td_threadtitle_4167489"]/div/span/a[last()]/@href'
        '''for item in items:
            print(item.extract())
            after_string = item.extract().replace('_', '',1)
            thread_id = item.extract().split('_')[2]
            print("thrad_id = " + thread_id)
            select_string = '//td[@id="td_'+after_string +'"]/div/span/a[last()]/@href'
            select_text_string = '//td[@class="alt1"]/div/a[@id="'+item.extract()+'"]/text()';
            items_thread_name = hxs.select(select_text_string)
            print(select_text_string)
            print("tuyenx");
            print(items_thread_name)
            for xxx in items_thread_name:
                thread_name = xxx.extract()
                print(thread_name)
            #print(select_string)
            lastpage = 0
            itemsx = hxs.xpath(select_string)
            for xx in itemsx:
                xxx = xx.extract()
                #print("tuyen")
                #print(xxx)
                conti = 0
                if xxx == "#" :
                    conti = 1;
                if conti == 0:
                    lastpage = xxx.split('=')[2]

                print(xxx.split('='))
            if int(lastpage) >= 5:
                add_thread_sql(thread_id, thread_name,lastpage)

                print("tuyenlog")
                print(thread_name.encode('utf8', 'ignore'))
                print(thread_id)
                print(lastpage)
                #link = item.extract()
           # print(link)
          #  yield Request(complete_url(link), callback=self.parse_category)'''

    def parse_category(self, response):
        """
        Parse Categories, example :
        http://www.bhinneka.com/aspx/products/pro_display_products.aspx?CategoryID=0115
        """
        hxs = HtmlXPathSelector(response)
        # HXS to Detail link inside td and a
        items = hxs.select('//div[@class="box"]/table/tr')
        for item in items:
            '''
            Save Our Item
            '''
            bhinneka = CrawlerBhinnekaItem()
            bhinneka['item_link'] = complete_url(item.select('td[1]/a/@href').extract()[0])
            bhinneka['item_name'] = encode(item.select('td[1]/a/text()').extract()[0])
            bhinneka['item_category'] = item.select('td[2]/text()').extract()[0]
            bhinneka['item_price'] = item.select('td[3]/text()').extract()[0]

            '''
            Save Our Item to MySQL
            '''
            insert_table(bhinneka)
