import os
import platform
import time

from .Driver import MongoDB, Urllib

DOWNLOAD_COMPLETED = 200

DOWNLOAD_STATUS_WAIT = 100
DIRNAME_MAX_SIZE = 200
FILENAME_MAX_SIZE = 50


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

    class Downloader:
        def __init__(self, parent, client, agent, db):
            self.parent = parent
            self.client = client
            self.agent = agent
            self.db = db

        def __insert_download_task(self, document):
            self.db.downloader.save(document)

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

        def __init_thumb(self, thumb):
            thumb["is_exist"] = True
            return self.db.image_pool.find_and_modify(
                {"thumb_id": thumb["thumb_id"], "is_exist": False},
                {"$set": thumb})

        def __push_thumb_image_names(self, thumb_id, filename):
            result = self.db.image_pool.find_and_modify({"thumb_id": thumb_id},
                                                        {"$push": {"image_names": filename}})
            return result

        def add(self, urls, thumb_id, thumb_path):
            task = dict()

            if thumb_id is None:
                task["thumb_id"] = self.parent.get_thumb_id()
            else:
                task["thumb_id"] = thumb_id

            if thumb_path is None:
                task["thumb_path"] = time.strftime("%Y_%m_%d")
            else:
                task["thumb_path"] = thumb_path

            task["targets"] = urls
            self.__insert_download_task(task)
            return task["thumb_id"]

        def start(self, success=None, failed=None):
            if callable(success):
                success = [success]
            if callable(failed):
                failed = [failed]

            if failed is None:
                failed = []
            if success is None:
                success = []
            _success = [self.__download_success]
            _success.extend(success)
            _failed = [self.__download_failed]
            _failed.extend(failed)

            download_tasks = self.__get_download_task()
            for task in download_tasks:
                for url in task["targets"]:
                    self.agent.get(url, success=_success, failed=_success,
                                   meta={"thumb_id": task["thumb_id"], "thumb_path": task["thumb_path"]})

        def check_downloader(self):
            return self.parent.check_downloader()

        def __download_success(self, response, success=None, failed=None):
            filename = response.url.split('/')[-1]

            thumb = dict()
            thumb["thumb_id"] = response.meta["thumb_id"]
            thumb["thumb_path"] = response.meta["thumb_path"] + os.path.sep + str(thumb["thumb_id"])

            self.__init_thumb(thumb)

            self.__push_thumb_image_names(thumb["thumb_id"], filename)
            self.__save_file(filename, response.read(),
                             self.parent.get_download_path() + os.path.sep + thumb["thumb_path"])

            count = self.__pull_download_targets(thumb["thumb_id"], response.url)
            if count is 0:
                thumb = self.parent.get_thumb_by_id(thumb["thumb_id"])
                thumb["image_names"].sort()
                self.__delete_download_task({"thumb_id": response.meta["thumb_id"]})
                for s_callback in success:
                    s_callback(
                        {"msg": "download completed", "code": DOWNLOAD_COMPLETED,
                         "details": thumb})
                self.parent.save_thumb(thumb)

        @staticmethod
        def __save_file(filename, data, save_path):
            print("file cached:" + save_path + os.path.sep + filename)
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            with open(save_path + os.path.sep + filename, "wb+") as file:
                file.write(data)

            return save_path

        def __download_failed(self, body, failed=None):
            pass
            # print(body)

        def default_send_download_msg(self):
            pass

    def get_thumb_id(self):
        info = self.db.image_cloud_info.find_and_modify({"info": "main"}, {"$inc": {"max_thumb_id": 1}})
        self.db.image_pool.insert({"thumb_id": info["max_thumb_id"], "is_exist": False})
        self.__update_image_cloud_info()
        return info["max_thumb_id"]

    def set_download_path(self, path):
        self.db.image_cloud_info.find_and_modify({"info": "main"}, {"$set": {"image_pool_path": path}})
        self.__update_image_cloud_info()

    def get_download_path(self):
        return self.image_cloud_info["image_pool_path"]

    def save_thumb(self, thumb):
        return self.db.image_pool.save(thumb)

    def add_task(self, urls, thumb_id=None, thumb_path=None):
        return self.downloader.add(urls, thumb_id, thumb_path)

    def start(self, success=None, failed=None):
        if success is None:
            success = self.downloader.default_send_download_msg
        if failed is None:
            failed = self.downloader.default_send_download_msg
        return self.downloader.start(success, failed)

    def get_thumb_by_id(self, thumb_id):
        return self.db.image_pool.find_one({"thumb_id": int(thumb_id)})

    def __initialize(self):
        self.__update_image_cloud_info()
        if not self.image_cloud_info:
            sys = platform.system()
            if sys == "Windows":
                default_local_path = "E:\\MyImage"
            elif sys == "Linux":
                default_local_path = "/volume1/Media/MyImage"
            else:
                return
            self.db.image_cloud_info.insert(
                {"info": "main", "version": "0.0.0.1", "max_thumb_id": 1, "image_pool_path": default_local_path})
            self.__update_image_cloud_info()

    def __update_image_cloud_info(self):
        self.image_cloud_info = self.db.image_cloud_info.find_one({"info": "main"})

    def check_downloader(self):
        return self.db.downloader.count()
