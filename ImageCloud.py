import os
import platform
import time

from bson import ObjectId

from .Driver import MongoDB, Urllib

# from PIL import Image

DOWNLOAD_COMPLETED = 200

DOWNLOAD_STATUS_WAIT = 100
DIRNAME_MAX_SIZE = 200
FILENAME_MAX_SIZE = 50

IMAGE_NOT_CACHED = 0
IMAGE_COVER_DOWNLOADING = 1
IMAGE_COVER_DOWNLOADED = 2
IMAGE_ALL_DOWNLOADING = 3
IMAGE_ALL_DOWNLOADED = 4

THUMB_ALL = 0
THUMB_COVER = 1


class ImageCloud:
    def __init__(self, username, password, host, port=27017, db_driver="Mongo", agent="Urllib"):
        self.uri = "mongodb"
        self.db_driver = db_driver
        self.agent = None
        if db_driver == "Mongo":
            self.client = MongoDB(username, password, host, port)
        if agent == "Urllib":
            self.agent = Urllib()
        self.db = self.client.image_cloud
        self.downloader = self.Downloader(self, self.client, self.agent, self.db)
        self.__initialize()

    # inner class about a downloader
    class Downloader:
        def __init__(self, parent, client, agent, db):
            self.parent = parent
            self.client = client
            self.agent = agent
            self.db = db

        def __insert_download_task(self, document):
            return self.db.downloader.insert_one(document).inserted_id

        def __get_download_task(self, query=None):
            if query is None:
                query = {}
            return self.db.downloader.find(query)

        def __delete_download_task(self, query):
            self.db.downloader.delete_one(query)

        def __pull_download_targets(self, thumb_id, url):
            result = self.db.downloader.find_and_modify({"thumb_id": thumb_id},
                                                        {"$pull": {"targets": url}})
            return len(result["targets"]) - 1

        def __download_success(self, response):
            filename = response.url.split('/')[-1]
            level = response.meta["level"]

            thumb_id = response.meta["thumb_id"]
            thumb_path = response.meta["thumb_path"]

            self.__save_file(filename, response.body, self.parent.get_download_path() + os.path.sep + thumb_path)

            count = self.__pull_download_targets(thumb_id, response.url)
            if count is 0:
                thumb = self.parent.get_thumb_by_id(thumb_id)
                thumb["image_names"].sort()
                self.__delete_download_task({"thumb_id": response.meta["thumb_id"]})
                if level == THUMB_ALL:
                    self.parent.change_status({"thumb_id": thumb["thumb_id"]}, IMAGE_ALL_DOWNLOADED)
                elif level == THUMB_COVER:
                    self.parent.change_status({"thumb_id": thumb["thumb_id"]}, IMAGE_COVER_DOWNLOADED)
                for s_callback in response.request.success:
                    s_callback(
                        {"msg": "download completed", "code": DOWNLOAD_COMPLETED,
                         "details": thumb})
                self.parent.save_thumb(thumb)

        def __download_failed(self, response):
            if response.status == 404:
                if not hasattr(response.request, "retry_form"):
                    if response.url.split(".")[-1] == "jpg":
                        self.retry_new_url(response, "png")
                    elif response.url.split(".")[-1] == "png":
                        self.retry_new_url(response, "jpg")
                    elif response.url.split(".")[-1] == "gif":
                        self.retry_new_url(response, "jpg")
                else:
                    if "jpg" not in response.request.retry_form:
                        self.retry_new_url(response, "jpg")
                    elif "png" not in response.request.retry_form:
                        self.retry_new_url(response, "png")
                    elif "gif" not in response.request.retry_form:
                        self.retry_new_url(response, "gif")
            else:
                pass

        def retry_new_url(self, response, form):
            new_url, new_image_name, source_image_name = self.__replace_request_form(response.url, form)
            task = self.db.downloader.find_one({"thumb_id": response.meta["thumb_id"]})
            for index, target in enumerate(task["targets"]):
                if target == response.url:
                    task["targets"][index] = new_url
                    break
            thumb = self.db.image_pool.find_one({"thumb_id": response.meta["thumb_id"]})
            self.db.downloader.save(task)
            for index, image_name in enumerate(thumb["image_names"]):
                if image_name.split(".")[0] == source_image_name.split(".")[0]:
                    thumb["image_names"][index] = new_image_name
                    break
            self.db.image_pool.save(thumb)
            print("Try url:" + response.url + " change to url: " + new_url)
            response.request.url = new_url
            response.request.retry_times += 1
            if hasattr(response.request, "retry_form"):
                response.request.retry_form.append(form)
            else:
                response.request.retry_form = [source_image_name.split(".")[-1], form]
            self.agent.task_processor(response.request)

        @staticmethod
        def __replace_request_form(url, form):
            replace_url = url.split("/")
            image_name = replace_url[-1]
            replace_name = image_name.split(".")
            replace_name[-1] = form
            replace_name = str.join(".", replace_name)
            replace_url[-1] = replace_name
            replace_url = str.join("/", replace_url)
            return replace_url, replace_name, image_name

        def add(self, urls, thumb_id, level):
            task = dict()

            if thumb_id is None:
                task["thumb_id"] = self.parent.get_thumb_id()
            else:
                task["thumb_id"] = thumb_id

            task["thumb_path"] = self.db.image_pool.find_one({"thumb_id": thumb_id})["thumb_path"]

            task["count"] = len(urls)
            task["targets"] = urls
            task["level"] = level
            return self.__insert_download_task(task)

        def start(self, task_id=None, success=None, failed=None):
            if callable(success):
                success = [success]
            if callable(failed):
                failed = [failed]
            if isinstance(task_id, str):
                task_id = ObjectId(task_id)
            if failed is None:
                failed = []
            if success is None:
                success = []
            _success = [self.__download_success]
            _success += success
            _failed = [self.__download_failed]
            _failed += failed

            if task_id is None:
                download_tasks = self.__get_download_task()
            else:
                download_tasks = self.__get_download_task({"_id": task_id})
            for task in download_tasks:
                for url in task["targets"]:
                    self.agent.send(url, success=_success, failed=_failed,
                                    meta=task)

        def check_downloader(self):
            return self.parent.check_downloader()

        def default_send_download_msg(self):
            pass

        @staticmethod
        def __save_file(filename, data, save_path):
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            with open(save_path + os.path.sep + filename, "wb+") as file:
                file.write(data)
                print("file cached:" + save_path + os.path.sep + filename)
            return save_path

        # @staticmethod
        # def valid_image(self, filename):
        #     valid = True
        #     with open(filename, 'rb') as file:
        #         buf = file.read()
        #         if buf[6:10] in (b'JFIF', b'Exif'):  # jpg图片
        #             if not buf.rstrip(b'\0\r\n').endswith(b'\xff\xd9'):
        #                 valid = False
        #         else:
        #             try:
        #                 Image.open(file).verify()
        #             except:
        #                 valid = False
        #     return valid

    def change_status(self, query, status):
        self.db.image_pool.find_and_modify(query, {'$set': {'status': status}})

    def get_thumb_by_status(self, status):
        return self.db.image_pool.find({"status": status})

    def get_thumb_id(self):
        info = self.db.image_cloud_info.find_and_modify({"info": "main"}, {"$inc": {"max_thumb_id": 1}})
        return info["max_thumb_id"]

    def insert_thumb(self, source_urls, cache_path):
        thumb = dict()
        thumb_id = self.get_thumb_id()
        thumb["thumb_id"] = thumb_id
        thumb["status"] = IMAGE_NOT_CACHED
        thumb["thumb_path"] = cache_path + os.path.sep + str(thumb_id)
        if source_urls and isinstance(source_urls, list):
            thumb["source"] = [str.join('/', source_urls[0].split('/')[:-1])]
            image_names = [url.split('/')[-1] for url in source_urls]
            try:
                image_names.sort(key=lambda x: int(x.split('.')[0]))
            except ValueError as e:
                image_names.sort()
            thumb["image_names"] = image_names
        self.db.image_pool.save(thumb)
        self.__update_image_cloud_info()
        return thumb_id

    def set_download_path(self, path):
        self.db.image_cloud_info.find_and_modify({"info": "main"}, {"$set": {"image_pool_path": path}})
        self.__update_image_cloud_info()

    def get_download_path(self):
        return self.image_cloud_info["image_pool_path"]

    def save_thumb(self, thumb):
        return self.db.image_pool.save(thumb)

    def add_task(self, urls, level, thumb_id=None):
        return self.downloader.add(urls, thumb_id, level)

    def start(self, task_id=None, success=None, failed=None):
        if success is None:
            success = self.downloader.default_send_download_msg
        if failed is None:
            failed = self.downloader.default_send_download_msg
        return self.downloader.start(task_id, success, failed)

    def get_thumb_by_id(self, thumb_id):
        return self.db.image_pool.find_one({"thumb_id": int(thumb_id)})

    def __initialize(self):
        self.__update_image_cloud_info()
        if not self.image_cloud_info:
            sys = platform.system()
            if sys == "Windows":
                default_local_path = "E:\\MyImage"
            elif sys == "Linux":
                default_local_path = "/var/www/image-pool"
            else:
                return
            self.db.image_cloud_info.insert(
                {"info": "main", "version": "0.0.0.1", "max_thumb_id": 1, "image_pool_path": default_local_path})
            self.__update_image_cloud_info()

    def __update_image_cloud_info(self):
        self.image_cloud_info = self.db.image_cloud_info.find_one({"info": "main"})

    def check_downloader(self):
        return self.db.downloader.count()

    def download_all(self, level=THUMB_ALL):
        if level == THUMB_COVER:
            thumbs = self.get_thumb_by_status(IMAGE_NOT_CACHED)
            for thumb in thumbs:
                self.change_status({"_id": thumb['_id']}, IMAGE_COVER_DOWNLOADING)
                urls = [thumb["source"][0] + "/" + image_names for image_names in thumb["image_names"][:3]]
                self.add_task(urls, level, thumb['thumb_id'])
            self.start()
        elif level == THUMB_ALL:
            pass

    def download(self, thumb_id, level=THUMB_ALL, immediate_sign=True):
        thumb = self.get_thumb_by_id(thumb_id)
        if thumb["status"] not in [IMAGE_COVER_DOWNLOADING, IMAGE_ALL_DOWNLOADING, IMAGE_ALL_DOWNLOADED]:
            if level == THUMB_COVER:
                self.change_status({"thumb_id": int(thumb_id)}, IMAGE_COVER_DOWNLOADING)
            elif level == THUMB_ALL:
                self.change_status({"thumb_id": int(thumb_id)}, IMAGE_ALL_DOWNLOADING)
            urls = [thumb["source"][0] + "/" + image_names for image_names in thumb["image_names"]]
            task_id = self.add_task(urls, level, int(thumb_id))
            if immediate_sign:
                return self.start(task_id)
            else:
                return task_id
        else:
            return self.db.downloader.find_one({"thumb_id": int(thumb_id)})["_id"]
