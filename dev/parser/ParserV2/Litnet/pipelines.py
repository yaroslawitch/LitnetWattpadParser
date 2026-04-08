# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


from scrapy.exceptions import DropItem

from domain.models import Book

from scrapy.exceptions import DropItem

from Litnet.preprocessing.techprep import preprocess

class BookPipeline:
    def __init__(self):
        self.seen_ids = set()  # set для текущего запуска

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        spider.logger.info("BookPipeline started.")

    def close_spider(self, spider):
        spider.logger.info("BookPipeline finished.")

    def process_item(self, item, spider):
        if not isinstance(item, Book):
            return item

        book_id = getattr(item, "id", None)
        if not book_id:
            raise DropItem("Book without book_id")

        # Дедупликация для текущего запуска
        if book_id in self.seen_ids:
            raise DropItem(f"Book {book_id} already processed in this run")

        self.seen_ids.add(book_id)

        #Техпредобработка
        item = preprocess(item)

        return item