from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from urllib import request as req
from urllib.error import HTTPError
from urllib.parse import quote

MAX_RETRY_TIMES = 5


class Urllib:
    def __init__(self, max_workers=20):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def send(self, url, method="GET", headers=None, success=None, failed=None, meta=None):
        request = self.Request(url, method, headers, success, failed, meta)
        self.submit(request)

    def submit(self, request):
        self.executor.submit(self.task_processor, request)

    def task_processor(self, request):
        handler = req.Request(quote(request.url, safe='/:?=&'), headers=request.headers, method=request.method)
        try:
            result = req.urlopen(handler)
            if 200 == result.status:
                print("URL: " + request.url + " <200>")
                response = self.Response(request, 200, result)
                request.success[0](response)
        except HTTPError as e:
            if 404 == e.code:
                print("URL: " + request.url + " <404>")
                response = self.Response(request, 404)
                request.failed[0](response)
            else:
                print("URL: " + request.url + " <" + str(e.code) + ">")
                if request.retry_times < MAX_RETRY_TIMES:
                    request.retry_times += 1
                    self.task_processor(request)
                else:
                    response = self.Response(request, e.code)
                    request.failed[0](response)

    class Request:
        def __init__(self, url, method=None, headers=None, success=None, failed=None, meta=None):
            if meta is None:
                meta = dict()
            if headers is None:
                headers = dict()
            self.url = url
            self.method = method
            self.headers = headers
            self.success = success
            self.failed = failed
            self.meta = meta
            self.retry_times = 0

    class Response:
        def __init__(self, request, status, result=None):
            self.request = request
            self.url = request.url
            self.meta = request.meta
            self.status = status

            if status == 200:
                self.body = result.read()
            else:
                self.body = None
