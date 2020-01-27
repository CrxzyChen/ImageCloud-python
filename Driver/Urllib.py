from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from urllib import request


class Urllib:
    def __init__(self, max_workers=20):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def get(self, url, headers=None, method=None, success=None, failed=None, meta=None):
        if meta is None:
            meta = dict()
        self.executor.submit(self.send, url, headers, "GET", success, failed, meta)

    @staticmethod
    def send(url, headers=None, method=None, success=None, failed=None, meta=None):
        if meta is None:
            meta = dict()
        if headers is None:
            headers = dict()
        req = request.Request(url, headers=headers, method=method)
        response = request.urlopen(req)
        if 200 == response.getcode():
            print("URL: " + response.url + " <200>")
            response.meta = meta
            success[0](response, success[1:])
        else:
            print("URL: " + response.url + " <" + str(response.getcode) + ">")
            failed(response)
