# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from wattpad_models import Tag, Rank, User, Comment, Chapter, Book
from Wattpad.preproc.techpreproc import clean_text, preproc, recursive_clean, items_to_Book_class

class WattpadPipeline:
    
    
    
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
        if item.get('title', False):
            item1 = preproc(item)
            item2 = recursive_clean(item1)
            item3 = items_to_Book_class(item2)
            
            
            book_id = getattr(item3, "id", None)
            if not book_id:
                raise DropItem("Book without book_id")

            # Дедупликация для текущего запуска
            if book_id in self.seen_ids:
                raise DropItem(f"Book {book_id} already processed in this run")

            self.seen_ids.add(book_id)
            
            return item3
        else:
            return item
