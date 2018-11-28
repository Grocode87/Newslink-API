"""
Main class for the process of adding articles to the database
"""
# pylint: disable=line-too-long
# pylint: disable=too-many-instance-attributes
import sys
import time
import datetime
import scrape
import MySQLdb

import multiprocessing
# import Process, Queue, Lock

# number of articles to process per batch
BATCH_SIZE = 10

# interval between article retrievals
INTERVAL_MINUTES = 30

# database values
DB_HOST = 'localhost'
DB_NAME = 'newsapp'
DB_USER = 'user'
DB_PASS = ''

class ArticleScrapeProcess(multiprocessing.Process):
    """
    Thread that gets an article, scrapes it, and adds its entities to a queue
    """
    def __init__(self, name, article_q, entity_q, exit_flag, thread_lock, new_article_texts, existing_article_texts, database_to_list):
        super(ArticleScrapeProcess, self).__init__()
        self._name = name
        self._article_queue = article_q
        self._entity_queue = entity_q
        self._thread_lock = thread_lock
        self._exit_flag = exit_flag

        self._new_article_texts = new_article_texts
        self._existing_article_texts = existing_article_texts
        self._database_to_list = database_to_list

        self._working = False
        self._scraper = scrape.Scraper()

        # connect to database
        self._db = MySQLdb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME, charset='utf8')
        self._cur = self._db.cursor()

        print("Started article thread", name)

    def run(self):
        """
        Indefinite loop that checks for any new articles and scrapes them
        """
        print("Running thread", self._name)
        while not self._exit_flag:
            # useless code that prints a line and removes it
            # needed to overcome weird bug where the thread was not running
            print("", end="", flush=True)

            # check if there is an avaliable article in the queue
            if self._article_queue.qsize() > 0:
                self._working = True
                print("Processing article", self._name)
                time.sleep(1)
                # get an article from the queue
                article = self._article_queue.get()
                self._thread_lock.acquire()
                for num in range(3):
                    self._entity_queue.put("entity-" + str(num))
                self._thread_lock.release()

                # see if article already exists
                self._cur.execute('SELECT COUNT(*) AS total FROM articles WHERE title=%s', [article['title']])
                if self._cur.fetchone()[0] <= 0:
                    title = self.fallback_value(article['title'], "")
                    print("Adding article:", title)
                    url = self.fallback_value(article['url'], "")
                    source = self.fallback_value(article['source']['name'], "")
                    img_url = self.fallback_value(article['urlToImage'], "")
                    desc = self.fallback_value(article['description'], "")
                    print("Getting text")
                    text = self._scraper.get_text(url)
                    cleaned_text = self._scraper.clean_text(text)

                    # determine if the article is valid
                    if len(text.split()) > 40 and title != None:
                        print("Getting category")
                        category = self._scraper.get_category(cleaned_text)
                        print("Getting entities")
                        entities = self._scraper.get_entities(text)

                         # acquire a thread lock to perform asyncronous tasks
                        print("getting thread lock")
                        self._thread_lock.acquire()
                        # insert the article into the database
                        print("adding to db")
                        self._cur.execute(""" INSERT INTO articles(title, description, text, url, img_url, date_created, category, source)
                                          VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """,
                                          (title, desc, cleaned_text, url, img_url, datetime.datetime.now(), category, source))
                        added_index = self._cur.lastrowid
                        print("commit")
                        self._db.commit()

                        # add entities to queue
                        for entity in entities:
                            self._entity_queue.put({'name':entity, 'rank':entities[entity]})

                        self._new_article_texts.append(cleaned_text)
                        self._database_to_list[added_index] = (len(self._existing_article_texts) + len(self._new_article_texts) - 1)

                        print("Added article:", title)

                        # release the thread lock
                        self._thread_lock.release()

            else:
                self._working = False

        print("Exiting thread", self._name)

    @staticmethod
    def fallback_value(value, fallback):
        """
        Method that returns value if it is not null, otherwise it returns the fallback value
        """
        if value is None:
            return fallback
        return value

    def is_working(self):
        """
        Return the working boolean
        """
        return self._working

    def set_working(self, working):
        """
        Set the working boolean
        """
        self._working = working


class EntityScrapeProcess(multiprocessing.Process):
    """
    Thread that gets entitities from the queue and adds them to the database
    """
    def __init__(self, name, entity_q, uploader):
        super(EntityScrapeProcess, self).__init__()
        self._name = name
        self._uploader = uploader
        self._entity_queue = entity_q
        self._working = False

    def run(self):
        """
        Indefinite loop that checks for entities in the queue and processes them
        """
        while not self._uploader.do_exit():
            # useless code that prints a line and removes it
            # needed to overcome weird bug where the thread was not running
            print("", end="", flush=True)

            # check if there is an avaliable entity in the queue
            if self._entity_queue.qsize() > 0:
                self._working = True
                

                # Scrape the entity and get required info
                entity = self._entity_queue.get()
                print('Processing entity:', entity['name'])
                time.sleep(1)

            else:
                self._working = False

        print("Exiting thread", self._name)

    def is_working(self):
        """
        Return the working boolean
        """
        return self._working

    def set_working(self, working):
        """
        Set the working boolean
        """
        self._working = working

class MultithreadedAdder:
    """
    Class that contains the brains behind adding articles
    Scrapes the articles and entities in a multithreaded way
    """
    def __init__(self, num_article_threads, num_entity_threads):

        self._article_q = multiprocessing.Queue()
        self._entity_q = multiprocessing.Queue()
        self._article_threads = []
        self._entity_threads = []
        self._thread_lock = multiprocessing.Lock()
        self._exit_flag = False
        self._article_getter = scrape.ArticleGetter()
        self._article_scraper = scrape.Scraper()

        self.list_to_database = {}
        self.clusters = {}

        manager = multiprocessing.Manager()
        self.thread_lock = manager.Lock()
        self._exit_flag = manager.Value('i', 0)
        self.existing_article_texts = manager.list()
        self.new_article_texts = manager.list()
        self.database_to_list = manager.dict()

        # connect to database
        self._db = MySQLdb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME, charset='utf8')
        self._cur = self._db.cursor()

        # create article scraping threads
        for i in range(num_article_threads):
            thread = ArticleScrapeProcess("a-" + str(i), self._article_q, self._entity_q, self._exit_flag, self.thread_lock, self.new_article_texts, self.existing_article_texts, self.database_to_list)
            thread.start()
            self._article_threads.append(thread)

        # create entity scraping threads
        for i in range(num_entity_threads):
            thread = EntityScrapeProcess("e-" + str(i), self._entity_q, self)
            thread.start()
            self._entity_threads.append(thread)

    def run(self):
        """
        Main loop for adding articles
        """
        while True:
            start_time = datetime.datetime.now()

            print("Adding new articles...")
            # reset variables for clustering
            self.existing_article_texts = []
            self.new_article_texts = []
            self.database_to_list = {}
            self.list_to_database = {}

            # get articles to add
            print("Getting articles to add")
            articles_to_add = self._article_getter.get_articles(60)

            # create local database instance
            print("Creating local database instance")
            self.clusters = self.create_local_db_instance()

            # add articles
            print("Adding articles to the database")
            self.add_articles(articles_to_add)

            # cluster articles

            # cluster calculations

            end_time = datetime.datetime.now()

            time_diff = end_time - start_time
            minutes_seconds = divmod(time_diff.days * 86400 + time_diff.seconds, 60)
            minutes_elapsed = minutes_seconds[0]

            sleep_time = INTERVAL_MINUTES - minutes_elapsed
            if sleep_time < 0:
                sleep_time = 0

        print("Adding took", minutes_elapsed, "minutes")
        print("Waiting", sleep_time, "minutes until next loop")

        time.sleep(sleep_time * 60)

    def add_articles(self, articles):
        """
        Runs the article adding process
        """
        step = 0

        while step < len(articles):
            print("Step:", step, ":", step+BATCH_SIZE, '  of', len(articles))

            batch_articles = articles[step:step+BATCH_SIZE]
            for article in batch_articles:
                self._article_q.put(article)

            self.start_threads(self._article_threads)
            self.start_threads(self._entity_threads)

            # wait for current batch of articles to be finished
            while not self.all_threads_done(self._article_threads):
                time.sleep(2)

            # wait for the entity queue to empty
            while not self.all_threads_done(self._entity_threads):
                time.sleep(2)

            step += BATCH_SIZE

        print("Done adding all articles and entities")
        # tell the threads to exit
        self._exit_flag = True

        # wait for article threads to finish
        for a_thread in self._article_threads:
            a_thread.join()

        # wait for entity threads to finish
        for e_thread in self._entity_threads:
            e_thread.join()
        print("Closed threads")

    def create_local_db_instance(self):
        """
        Returns a dict representation of the last 24 hours of database story clusters
        """
        clusters = {}
        self._cur.execute("SELECT * from clusters WHERE last_updated > (NOW() - INTERVAL 24 HOUR)")
        for cluster in self._cur.fetchall():
            clusters[cluster[0]] = {}
            self._cur.execute(""" SELECT * FROM articles WHERE id IN
                              (SELECT article_id FROM `cluster details` WHERE cluster_id = %s)""",
                              [cluster[0]])

            c_articles = self._cur.fetchall()
            for article in c_articles:
                clusters[cluster[0]][article[0]] = {
                    'title': article[1],
                    'text': article[3]
                }
                self.existing_article_texts.append(self._article_scraper.clean_text(article[3]))
                self.database_to_list[article[0]] = (len(self.existing_article_texts) - 1)

        return clusters

    def do_exit(self):
        """
        Returns the exit flag
        """
        return self._exit_flag

    def get_thread_lock(self):
        """
        Returns the thread lock
        """
        return self._thread_lock

    @staticmethod
    def start_threads(thread_list):
        """
        Tell every thread to start working
        """
        for thread in thread_list:
            thread.set_working(True)

    @staticmethod
    def all_threads_done(thread_list):
        """
        Return whether every thread is done working
        """
        done = True
        for thread in thread_list:
            if thread.is_working():
                done = False
        return done

def main():
    """
    Main entry point for the script.
    """
    print("Creating adder... get ready!")
    time.sleep(2)
    adder = MultithreadedAdder(2, 0)

    print("3, 2, 1, GOOOOOO!")
    adder.run()

if __name__ == '__main__':
    sys.exit(main())
