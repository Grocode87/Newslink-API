"""
Article adding and clustering with multiprocessing pools and MySQLdb
"""
# pylint: disable=line-too-long

import sys
import multiprocessing
import datetime
import random
import time
import MySQLdb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import scrape

# interval between article retrievals
INTERVAL_MINUTES = 90

# article scraping batch size
BATCH_SIZE = 50

# interval between progress logs for article scraping
LOGGING_INTERVALS = 1

# database values
DB_HOST = 'localhost'
DB_NAME = 'newsapp'
DB_USER = 'user'
DB_PASS = ''

def fallback_value(value, fallback):
    """
    Method that returns value if it is not null, otherwise it returns the fallback value
    """
    if value is None:
        return fallback
    return value

def process_article(article):
    """
    Scrape an article and add it to the database
    """
    # connect to database
    database_obj = MySQLdb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME, charset='utf8')
    cur = database_obj.cursor()

    return_obj = {}
    scraper = scrape.Scraper()

     # see if article already exists
    cur.execute('SELECT COUNT(*) AS total FROM articles WHERE title=%s', [article['title']])
    if cur.fetchone()[0] <= 0:
        # Scrape article
        title = fallback_value(article['title'], "")
        url = fallback_value(article['url'], "")
        source = fallback_value(article['source']['name'], "")
        img_url = fallback_value(article['urlToImage'], "")
        desc = fallback_value(article['description'], "")
        text = scraper.get_text(url)
        cleaned_text = scraper.clean_text(text)

        # determine if the article is valid
        if len(text.split()) > 40 and title != None:
            category = scraper.get_category(cleaned_text)
            entities = scraper.get_entities(text)

            # insert the article into the database
            cur.execute(""" INSERT INTO articles(title, description, text, url, img_url, date_created, category, source)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s) """,
                        (title, desc, cleaned_text, url, img_url, datetime.datetime.now(), category, source))
            added_index = cur.lastrowid

            # commit database changes
            database_obj.commit()

            # add the article's entities
            for entity in entities:
                while True:
                    try:
                        process_entity(entity, entities[entity], added_index, cur)
                    except MySQLdb.OperationalError:
                        continue
                    break

            # commit database changes
            database_obj.commit()

            return_obj = {
                'database_idx': added_index,
                'title': title,
                'text': text,
                'cleaned_text': cleaned_text,
                'desc': desc,
                'source': source,
                'url': url,
                'img_url': img_url,
                'category': category,
                'entities': entities
            }

    return return_obj

def process_entity(name, score, article_idx, cursor):
    """
    Add the entity to the database
    """
    # Make sure entity name is utf-8
    name = name.encode('utf-8').decode('utf-8')
    rank = score

    # see if the entity already exists
    cursor.execute("SELECT id, total_occurences FROM entities WHERE name=%s", [name])
    returned = cursor.fetchall()
    if returned != ():
        # if the entity exists, add one to the occurence
        new_occurences = returned[0][1] + 1

        #cursor.execute("UPDATE entities SET total_occurences = %s WHERE id IN (SELECT id FROM entities WHERE name=%s order by id)",
        #                (new_occurences, name))
        cursor.execute("UPDATE entities SET total_occurences = %s WHERE name = %s",
                       (new_occurences, name))

        entity_index = returned[0][0]

    else:
        # if the entity doesn't exist, add it to the database
        cursor.execute("INSERT INTO entities(name, total_occurences) VALUES(%s, %s)",
                       (name, 1))
        entity_index = cursor.lastrowid

    # link the entity to its article
    cursor.execute("""INSERT INTO entity_details(article_id, entity_id, score)
                    VALUES(%s, %s, %s)""",
                   (article_idx, entity_index, rank))

    # add to entity frequency
    cursor.execute("""INSERT INTO entity_frequency(entity_id)
                    VALUES(%s)""",
                   [entity_index])

def run_cluster_calculations(cur, db_obj):
    """
    Method to calculate cluster rank and category
    """
    # Select every clustser that was updated in the last 24 hours
    cur.execute("SELECT * from clusters WHERE last_updated > (NOW() - INTERVAL 24 HOUR)")
    all_clusters = cur.fetchall()

    for cluster in all_clusters:
        cur.execute("SELECT * FROM articles WHERE id IN (SELECT article_id FROM `cluster details` WHERE cluster_id = %s)", [cluster[0]])
        all_articles = cur.fetchall()

        # find the top category
        all_categories = [row[7] for row in all_articles]
        top_category = max(set(all_categories), key=all_categories.count)

        # calculate rank based on when the cluster's articles where posted
        curr_date = datetime.datetime.now()
        rank = 0
        for article in all_articles:
            rank += 1
            last_date = article[6]
            time_between = curr_date - last_date
            hours_between = (time_between.days * 24) + (time_between.seconds / 3600)
            rank -= (hours_between / 24)
            if rank < 0:
                rank = 0

        # update the cluster's database entry
        cur.execute("UPDATE clusters SET category=%s, rank=%s WHERE id=%s", (top_category, rank, cluster[0]))

    db_obj.commit()

def maintain_entity_freqs(cursor, database_obj):
    """
    Remove entity frequency entries older than 30 days
    """
    cursor.execute("DELETE FROM entity_frequency WHERE date < (NOW() - INTERVAL 30 DAY)")
    database_obj.commit()

def run(do_truncate=False):
    """
    Main function that runs article adding
    """
    # connect to the database
    print("Connecting to database ...")
    database_obj = MySQLdb.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME, charset='utf8')
    cur = database_obj.cursor()

    article_getter = scrape.ArticleGetter()
    scraper = scrape.Scraper()
    
    if do_truncate:
        print("Truncating database tables ...")
        cur.execute("RENAME TABLE articles TO t1")
        cur.execute("CREATE TABLE articles LIKE t1")
        cur.execute("DROP TABLE t1;")

        cur.execute("TRUNCATE TABLE entities")
        cur.execute("TRUNCATE TABLE entity_details")
        cur.execute("TRUNCATE TABLE entity_frequency")
        cur.execute("TRUNCATE TABLE clusters")
        cur.execute("TRUNCATE TABLE `cluster details`")

        database_obj.commit()

    # get the articles that need to be processed
    print("Getting articles ...")
    articles = article_getter.get_articles(INTERVAL_MINUTES * 2)

    # pooled batch processing with accurate progress logging
    print("Scraping articles ...")
    processed_articles = []
    pool = multiprocessing.Pool()

    step = 0
    next_logged_percent = LOGGING_INTERVALS

    while (step * BATCH_SIZE) <= len(articles):
        # Get list of articles for batch
        batch_articles = articles[step * BATCH_SIZE : (step * BATCH_SIZE) + BATCH_SIZE]

        # pool the article processing
        processed_batch = pool.map(process_article, batch_articles)
        processed_articles.extend(processed_batch)

        # calculate the current process
        progress = ((step * BATCH_SIZE) + len(batch_articles)) / len(articles) * 100

        step += 1

        # determine whether to print log info and calculate log percent
        new_log = False
        while progress >= next_logged_percent:
            next_logged_percent += LOGGING_INTERVALS
            new_log = True

        # print log info if LOGGING INTERVAL is passed
        if new_log:
            print("BATCH #" + str(step - 1) + " Complete  (" + str(((step - 1) * BATCH_SIZE) + len(batch_articles)) + "/" + str(len(articles)) + ")")
            print(next_logged_percent - LOGGING_INTERVALS, "% Done")

    # remove empty articles
    processed_articles = [article for article in processed_articles if article != {}]
    print("Articles remaining after scraping:", len(processed_articles))

    # lists of text for similarity calculations
    print("Clustering ...")
    new_article_texts = []
    existing_article_texts = []

    # dicts to link list text entries to their actual article
    database_to_list = {}
    list_to_database = {}

    # create a local database instance
    print("Creating local db instance ...")
    clusters = {}
    cur.execute("SELECT * from clusters WHERE last_updated > (NOW() - INTERVAL 24 HOUR)")
    for cluster in cur.fetchall():
        clusters[cluster[0]] = {}
        cur.execute(""" SELECT * FROM articles WHERE id IN
                        (SELECT article_id FROM `cluster details` WHERE cluster_id = %s)""", [cluster[0]])

        c_articles = cur.fetchall()
        for article in c_articles:
            clusters[cluster[0]][article[0]] = {
                'title': article[1],
                'text': article[3]
            }
            existing_article_texts.append(scraper.clean_text(article[3]))
            database_to_list[article[0]] = (len(existing_article_texts) - 1)

    # go through new articles, prepare them for clustering
    print("Preparing new articles for clustering ...")
    for article in processed_articles:
        new_article_texts.append(article['cleaned_text'])
        database_to_list[article['database_idx']] = (len(existing_article_texts) + len(new_article_texts) - 1)

    list_to_database = {v: k for k, v in database_to_list.items()}

    # Run the clustering
    print("\n---- clustering start")
    if new_article_texts != []:
        all_article_texts = existing_article_texts + new_article_texts

        # vectorize the input texts
        vectorizer = TfidfVectorizer()
        compare_vector = vectorizer.fit_transform(all_article_texts)
        new_vector = vectorizer.transform(new_article_texts)

        # calculate cosine similarity between new articles and all articles
        print("calculating cosine similarity")
        simililarities = cosine_similarity(new_vector, compare_vector)

        # go through every new article and check if it should be added to an existing cluster
        print("going through every article")
        for new_article_i, new_article_text in enumerate(new_article_texts):
            database_article_id = list_to_database[new_article_i + len(existing_article_texts)]
            added = False
            for cluster in clusters:
                if not added:
                    is_matching = False
                    for article in clusters[cluster]:
                        list_index = database_to_list[article]
                        sim = simililarities[new_article_i][list_index]

                        if sim > 0.50:
                            is_matching = True

                    if is_matching:
                        added = True
                        cur.execute("SELECT title FROM articles where id=%s", [database_article_id])
                        title = cur.fetchall()[0][0]
                        clusters[cluster][database_article_id] = {
                            'title': title,
                            'text': new_article_text
                        }
                        cur.execute("UPDATE clusters SET last_updated=%s WHERE id=%s", (datetime.datetime.now(), cluster))
                        cur.execute("INSERT INTO `cluster details`(cluster_id, article_id) VALUES(%s, %s)", (cluster, database_article_id))

            if not added:
                # if a matching cluster was not found, create a new cluster and add the article
                cur.execute("SELECT source FROM articles where id=%s", [database_article_id])
                source = cur.fetchall()
                if len(source) > 0:
                    if source[0][0] != "Independent":
                        # new cluster
                        cur.execute("INSERT INTO clusters(last_updated, random) VALUES(%s, %s)", [datetime.datetime.now(), random.randint(0, 99)])
                        new_cluster_index = cur.lastrowid

                        # add article to cluster
                        cur.execute("INSERT INTO `cluster details`(cluster_id, article_id) VALUES(%s, %s)", (new_cluster_index, database_article_id))

                        # add cluster and article to local cluster instance
                        cur.execute("SELECT title FROM articles where id=%s", [database_article_id])
                        clusters[new_cluster_index] = {}
                        clusters[new_cluster_index][database_article_id] = {
                            'title': cur.fetchall()[0][0],
                            'text': new_article_texts[new_article_i]
                        }
                else:
                    print(database_article_id, "does not exist")
        print("clustering completed")
        print("---- clustering end\n")

    # commit database changes from clustering
    database_obj.commit()

    # run cluster calculations
    print("Running cluster calculations ...")
    run_cluster_calculations(cur, database_obj)

    # manage entity frequency
    print("Running entity frequency maintence")
    maintain_entity_freqs(cur, database_obj)

    print("Finished cluster adding!")

def main():
    """
    Main entry point for the script.
    """
    while True:
        start_time = datetime.datetime.now()
        run()
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

if __name__ == "__main__":
    sys.exit(main())
