#!/usr/bin/python
#-*- coding: utf-8 -*-
import os
import json
import functools
import requests
import urllib.request
from urllib.request import FancyURLopener
import urllib.parse
import urllib.error
from lxml import etree
import time
from pymongo import MongoClient

from concurrent.futures import ProcessPoolExecutor, as_completed,ThreadPoolExecutor
import asyncio
import aiohttp

TsHeader = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

PATH = r'E:\a_kelub\Spider_PoolExecutor\xxx'
CHUNK_SIZE = 1024

P_MAX_WORKERS_PAGE = 3
P_MAX_WORKERS_IMGSAVE = 10

T_MAX_WORKERS_PAGE = 3
T_MAX_WORKERS_IMGSAVE = 10

LOCK_TIME = 0.2
EVENT_TIME = 0.2

SEMA_NUM = 3

class DataBase():
    def __init__(self):
        client = MongoClient('127.0.0.1', 27017)
        self._db = client['jandan']

class JDBase():
    def __init__(self):
        self._chunk_size = CHUNK_SIZE
        try:
            db = DataBase()
            self._db = db._db
        except:
            print('数据库连接失败')
            self._db = None

    def insert_header(self,header, img_url, call_url):
        referer = call_url
        if call_url.find('#') > 0:
            referer = call_url[:len(call_url) - 9]
        host = img_url[7:21]
        header.addheader("Host", host)
        header.addheader("User-Agent",
                         "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36")
        if img_url.find('.gif') > 0:
            header.addheader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        else:
            header.addheader("Accept", "*/*")
        header.addheader("Accept-Language", "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3")
        header.addheader("Accept-Encoding", "gzip,deflate")
        header.addheader("Referer", referer)
        header.addheader("Connection", "keep-alive")
        header.addheader("Upgrade-Insecure-Requests", "1")
        header.addheader("If-Modified-Since", """Sun, 26 Feb 2017 03: 53:17 GMT""")
        header.addheader('If-None-Match', '"F57B886E1C77028F85FAA6F665CD559E"')
        header.addheader("Cache-Control", "max-age=0")
        return header
    
    def GetUrl(self,url, data=None):
        f = ""
        if data: f = "?" + urllib.parse.urlencode(data)
        curl = url + f
        try:
            req = urllib.request.Request(curl, headers=TsHeader)
            response = urllib.request.urlopen(req)
            html = response.read()
        except Exception as e:
            print(e)
            print(curl)
            return False
        return html

    def PostUrl(self,url, data):
        data = urllib.parse.urlencode(data).encode('utf-8')
        try:
            request = urllib.request.Request(url, headers=TsHeader)
            request.add_header("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
            f = urllib.request.urlopen(request, data)
            a = f.read().decode('utf-8')
        except Exception as e:
            print(e)
            print(url)
            return False
        return a

    def page_list(self):
        p = ('72', 'http://jandan.net/ooxx/page-')
        pagelist = []
        for i in range(10):
            t = (str(int(p[0]) - i), p[1] + str(int(p[0]) - i))
            pagelist.append(t)
        return pagelist

    def url_dict_onepage(self,agrs):
        num = agrs[0]
        page = agrs[1]
        pagedict = {}
        r = self.GetUrl(page)
        html = etree.HTML(r)
        pic_lis = html.xpath('//ol//li//img/@src')
        git_lis = html.xpath('//ol//li//img/@org_src')
        git_lis.extend(pic_lis)
        pagedict['page'] = num
        pagedict['urls'] = git_lis
        return pagedict

    def url_list_db(self):
        urls = []
        try:
            db_urls = self._db.jd_url.find({}, {'page': 1, 'urls': 1})
        except:
            print('db find error')
            return False
        for url in db_urls:
            urls.extend(url['urls'])
        return urls

    def file_download(self,url):
        urlx = 'http:' + url
        imgname = url.split('/')[-1]
        if imgname.split('.')[-1] == 'gif':
            imgPath = os.path.join(PATH, 'gif', imgname)
        else:
            imgPath = os.path.join(PATH, 'pic', imgname)
        if not os.path.lexists(imgPath):
            # urllib.request.urlretrieve(urlx, imgPath)
            opener = FancyURLopener()
            opener.addheaders.clear()
            opener = self.insert_header(opener, urlx, imgPath)
            with open(imgPath, 'wb+') as f:
                while True:
                    chunk = opener.open(urlx,self._chunk_size)
                    if not chunk: break
                    f.write(chunk)

class ProcessPE(JDBase):
    def __init__(self):
        super(ProcessPE, self).__init__()
        self._max_workers_page = P_MAX_WORKERS_PAGE
        self._max_workers_imgsave = P_MAX_WORKERS_IMGSAVE

    def page_ProcessPE(self,urls):
        with ProcessPoolExecutor(max_workers=self._max_workers_page) as executor:
            future_to_url = {executor.submit(self.url_dict_onepage, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    self._db.jd_url.insert_one(result)
                except Exception as e:
                    print('raise an exception: {}'.format(e))
                else:
                    print('[db]{}'.format(result))

    def img_download_ProcessPE(self,urls):
        with ProcessPoolExecutor(max_workers=self._max_workers_imgsave) as executor:
            future_to_url = {executor.submit(self.file_download, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                except Exception as e:
                    print('raise an exception: {}'.format(e))
                else:
                    print('[save]OK')

    def Execute(self):
        page_list = self.page_list()
        self.page_ProcessPE(page_list)
        urls = self.url_list_db()
        if not urls:
            self.img_download_ProcessPE(urls)
        else:
            print('数据库无数据')

class ThreadPE(JDBase):
    def __init__(self):
        super(ThreadPE, self).__init__()
        self._max_workers_page = T_MAX_WORKERS_PAGE
        self._max_workers_imgsave = T_MAX_WORKERS_IMGSAVE

    def page_ThreadPE(self,urls):
        with ThreadPoolExecutor(max_workers=self._max_workers_page) as executor:
            future_to_url = {executor.submit(self.url_dict_onepage, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    self._db.jd_url.insert_one(result)
                except Exception as e:
                    print('raise an exception: {}'.format(e))
                else:
                    print('[db]{}'.format(result))

    def img_download_ThreadPE(self,urls):
        with ThreadPoolExecutor(max_workers=self._max_workers_imgsave) as executor:
            future_to_url = {executor.submit(self.file_download, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                except Exception as e:
                    print('raise an exception: {}'.format(e))
                else:
                    print('[save]OK')

    def Execute(self):
        page_list = self.page_list()
        self.page_ThreadPE(page_list)
        urls = self.url_list_db()
        # print(urls)
        if urls:
            self.img_download_ThreadPE(urls)
        else:
            print('数据库无数据')

'''
asyncio
'''
class Async_only(JDBase):
    def __init__(self):
        super(Async_only, self).__init__()
        self._pagedict = {}

    async def url_dict_onepage_Async(self, agrs):
        print('[A]',agrs)
        num = agrs[0]
        page = agrs[1]
        pagedict = {}
        # 异步调用
        # 这里只是例子 正确做法是用aiohttp这个异步网络库，而不是urllib这种阻塞库
        #         r = await self.GetUrl(page)
        #         html = etree.HTML(r)
        #         pic_lis = html.xpath('//ol//li//img/@src')
        #         git_lis = html.xpath('//ol//li//img/@org_src')
        #         git_lis.extend(pic_lis)
        #         pagedict['page'] = num
        #         pagedict['urls'] = git_lis
        #         print(pagedict)
        # 用异步sleep(1)模拟异步io操作
        # r = await asyncio.sleep(2)
        # 用time.sleep(1)模拟阻塞io操作,和同步执行一样
        r = await time.sleep(2)
        pagedict['page'] = num
        pagedict['urls'] = page
        print('[B]',pagedict)

    def page_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        tasks = [self.url_dict_onepage_Async(agrs) for agrs in urls]
        # print(tasks)
        # 执行coroutine
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

    def Execute(self):
        page_list = self.page_list()
        # print(page_list)
        self.page_ELoop(page_list)

'''
为了不修改之前代码可用,可以采用装饰器
带参数装饰器，修辞 url_dict_onepage_async()函数添加带信号量
'''
def asyncio_Semaphore(sema):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            with await asyncio.Semaphore(sema):
                await func(*args, **kwargs)
        return wrapper
    return decorator

'''
asyncio + aiohttp
替换阻塞io为异步比如 请求 比如 数据库操作等
'''
class AsyncAiohttp(JDBase):
    def __init__(self):
        super(AsyncAiohttp,self).__init__()
        self._chunk_size = CHUNK_SIZE

    def page_list(self):
        p = ('74', 'http://jandan.net/ooxx/page-')
        pagelist = []
        for i in range(2):
            t = (str(int(p[0]) - i), p[1] + str(int(p[0]) - i))
            pagelist.append(t)
            # print(a)
        return pagelist

    # @asyncio_Semaphore(3)
    async def url_dict_onepage_async(self,agrs):
        pagedict = {}
        num = agrs[0]
        page = agrs[1]
        async with aiohttp.request('GET',page) as r :
            data = await r.read()
        data = data.decode()
        html = etree.HTML(data)
        pic_lis = html.xpath('//ol//li//img/@src')
        git_lis = html.xpath('//ol//li//img/@org_src')
        git_lis.extend(pic_lis)
        pagedict['page'] = num
        pagedict['urls'] = git_lis
        self._db.jd_url.insert_one(pagedict) #TODO 替换成一次插入多值

    async def file_download_async(self, url):
        urlx = 'http:' + url
        imgname = url.split('/')[-1]
        if imgname.split('.')[-1] == 'gif':
            imgPath = os.path.join(PATH, 'gif', imgname)
        else:
            imgPath = os.path.join(PATH, 'pic', imgname)
        if not os.path.lexists(imgPath):
            # urllib.request.urlretrieve(urlx, imgPath)
            # async with aiohttp.request('GET', urlx) as r:
            async with aiohttp.ClientSession() as session:
                async with session.get( urlx) as r:
                    with open(imgPath, 'wb+') as f:
                        while True:
                            chunk = await r.content.read(self._chunk_size)
                            if not chunk:break
                            f.write(chunk)

    def page_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        tasks = [self.url_dict_onepage_async(url) for url in urls]
        # 执行coroutine
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

    def img_download_ELoop(self,urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        tasks = [self.file_download_async(url) for url in urls]
        # 执行coroutine
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

    def Execute(self):
        page_list = self.page_list()
        self.page_ELoop(page_list)
        urls = self.url_list_db()
        if urls:
            self.img_download_ELoop(urls)
        else:
            print('数据库无数据')

'''
Lock（锁）

'''
class AsyncAiohttp_Lock(AsyncAiohttp):
    def __init__(self):
        super(AsyncAiohttp_Lock,self).__init__()
        self._time_lock = LOCK_TIME

    def unlock(self,lock):
        lock.release()

    async def url_dict_onepage_async(self,*agrs):
        with await agrs[1]:
            await super(AsyncAiohttp_Lock,self).url_dict_onepage_async( agrs[0])

    async def tasks_lock(self,loop,urls):
        #创建一个锁
        lock = asyncio.Lock()
        #加锁
        await lock.acquire()
        #loop.call_later(delay, func, *args)延迟delay秒之后再执行。
        #loop.call_at(when, func, *args) 某个时刻才执行。
        #loop.call_soon( func, *args)顺序执行

        #延迟self._time_lock 时间后执行 解锁
        #事件循环变慢
        loop.call_later(self._time_lock,functools.partial(self.unlock,lock))
        await asyncio.wait([self.url_dict_onepage_async(url,lock) for url in urls])

    def page_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        # tasks = [self.url_dict_onepage_async(url) for url in urls]
        # print(tasks)
        # 执行coroutine
        loop.run_until_complete(self.tasks_lock(loop,urls))
        loop.close()

'''
Event（事件）
Class implementing event objects. An event manages a flag that can be set to true with the set() method
and reset to false with the clear() method. The wait() method blocks until the flag is true.
The flag is initially false.

clear()
Reset the internal flag to false. Subsequently, coroutines calling wait() will block
until set() is called to set the internal flag to true again.

is_set()
Return True if and only if the internal flag is true.

set()
Set the internal flag to true. All coroutines waiting for it to become true are awakened.
Coroutine that call wait() once the flag is true will not block at all.

coroutine wait()
Block until the internal flag is true.

If the internal flag is true on entry, return True immediately. Otherwise,
block until another coroutine calls set() to set the flag to true, then return True.

This method is a coroutine.

'''
class AsyncAiohttp_Event(AsyncAiohttp):
    def __init__(self):
        super(AsyncAiohttp_Event,self).__init__()
        self._time_event = EVENT_TIME

    async def url_dict_onepage_async(self,*agrs):
        print('{} waiting for event'.format(agrs[0]))
        await agrs[1].wait()
        await super(AsyncAiohttp_Event,self).url_dict_onepage_async( agrs[0])
        print('{} triggered'.format(agrs[0]))

    def set_event(self,event):
        print('setting event in callback')
        event.set()

    async def tasks_event(self, loop, urls):
        event = asyncio.Event()
        print('event start state:{}'.format((event.is_set())))

        loop.call_later(self._time_event,functools.partial(self.set_event, event))
        test_event = [self.url_dict_onepage_async(url,event) for url in urls]
        await asyncio.wait(test_event)
        print('event end state: {}'.format(event.is_set()))

    def page_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        # tasks = [self.url_dict_onepage_async(url) for url in urls]
        # print(tasks)
        # 执行coroutine
        loop.run_until_complete(self.tasks_event(loop,urls))
        loop.close()

'''
Condition（条件）
This class implements condition variable objects.
A condition variable allows one or more coroutines to wait until they are notified by another coroutine.
该类实现条件变量对象。条件变量允许一个或多个协同程序等待，直到被另一个协程通知。
'''
class AsyncAiohttp_Condition(AsyncAiohttp):
    def __init__(self):
        super(AsyncAiohttp_Condition,self).__init__()

    async def consumer(self,cond,name,second):
        await asyncio.sleep(second)
        with await cond:
            await cond.wait()
            print('{}: Resource is available to consumer'.format(name))

    async def producer(self,cond):
        await asyncio.sleep(2)
        with await cond:
            print('notify_all 一次性通知消费者')
            cond.notify_all()

    async def tasks_lock(self, loop, urls):
        condition = asyncio.Condition()
        task = loop.create_task(self.producer(condition))
        conditions = [self.consumer(condition,name,index) for index,name in enumerate(('c1','c2'))]
        await asyncio.wait(conditions)
        task.cancel()

    def page_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        # tasks = [self.url_dict_onepage_async(url) for url in urls]
        # print(tasks)
        # 执行coroutine
        loop.run_until_complete(self.tasks_lock(loop,urls))
        loop.close()

'''
Semaphore（信号量）
限制一次性请求数量
控制并发量
'''


class AsyncAiohttp_Semaphore(AsyncAiohttp):
    def __init__(self):
        super(AsyncAiohttp,self).__init__()
        self._sema = SEMA_NUM

    '''
    或者复写方法
    '''
    async def url_dict_onepage_async(self, agrs):
        with await asyncio.Semaphore(self._sema):
            await super(AsyncAiohttp_Semaphore,self).url_dict_onepage_async(agrs)

'''
前几个方案都是用两个loop 一个负责获取多个页面图片url并存入数据库，从数据库获取图片url 开启另一个loop 去下载图片

Queue 队列
urls -> queue -> download
'''
class AsyncAiohttp_Queue(AsyncAiohttp):
    def __init__(self):
        super(AsyncAiohttp_Queue,self).__init__()

    #consume
    async def file_download(self,queue):
        while True:
            print('[consume]wait for an pagedict from the producer')
            pagedict = await queue.get()
            print('[consume] get pagedict from queue {} '.format(pagedict))
            urls = pagedict.get('urls')
            #
            for url in urls:
                # print('[consume] download {}'.format(url))
                await self.file_download_async(url)
            print('[consume] Notify the queue that the pagedict has been processed')
            queue.task_done()

    #produce
    async def url_dict_onepage_async(self,*agrs):
        queue, urls = agrs[0],agrs[1]
        for url in urls:
            pagedict = {}
            print('[produce]getting img_urls from pageurl {}'.format(url))
            num = url[0]
            page = url[1]
            async with aiohttp.request('GET', page) as r:
                data = await r.read()
            data = data.decode()
            html = etree.HTML(data)
            pic_lis = html.xpath('//ol//li//img/@src')
            git_lis = html.xpath('//ol//li//img/@org_src')
            git_lis.extend(pic_lis)
            pagedict['page'] = num
            pagedict['urls'] = git_lis
            # put the item in the queue
            print('[produce]put the pagedict in the queue {}'.format(url))
            await queue.put(pagedict)

    async def tasks_queue(self, loop, urls):
        queue = asyncio.Queue()
        '''
        类似Task
        create_task是AbstractEventLoop的抽象方法，不同的loop可以实现不同的创建Task方法，这里用的是BaseEventLoop的实现。
        ensure_future是asyncio封装好的创建Task的函数，它还支持一些参数，甚至指定loop。一般应该使用它，
        除非用到后面提到的uvloop这个第三方库。
        '''
        # schedule the consumer  create Task
        file_download =  asyncio.ensure_future(self.file_download(queue))
        # run the producer and wait for completion
        await self.url_dict_onepage_async(queue,urls)
        # wait until the consumer has processed all items
        await queue.join()
        # the consumer is still awaiting for an item, cancel it
        file_download.cancel()

    def queue_ELoop(self, urls):
        # 获取EventLoop
        loop = asyncio.get_event_loop()
        # tasks = [self.url_dict_onepage_async(url) for url in urls]
        # print(tasks)
        # 执行coroutine
        loop.run_until_complete(self.tasks_queue(loop,urls))
        loop.close()

    def Execute(self):
        page_list = self.page_list()
        # print(page_list)
        self.queue_ELoop(page_list)

if __name__ == '__main__':
    start = time.time()
    # AA = ProcessPE()

    # AA = ThreadPE()

    # AA = Async_only()

    # AA = AsyncAiohttp()
    # AA = AsyncAiohttp_Lock()
    # AA = AsyncAiohttp_Semaphore()
    # AA = AsyncAiohttp_Condition()

    AA = AsyncAiohttp_Queue()

    AA.Execute()
    print('[endtime][{}]'.format(time.time() - start))