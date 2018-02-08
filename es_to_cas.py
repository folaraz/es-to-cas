from cassandra.cluster import Cluster
import random
import elasticsearch

cluster = Cluster()
session = cluster.connect()
cql_transaction = []


def connect_elasticsearch():
    es = elasticsearch.Elasticsearch()
    return es


def search_for_esdata(es):
    res = es.search(index="food", doc_type="fruit", body={"query": {"match_all": {}}}, size=10)
    random.seed(1)
    data = res["hits"]["hits"]
    print("Got Hits:", res["hits"]["total"])
    return data


def create_keyspace_table():
    session.execute("CREATE KEYSPACE IF NOT EXISTS food WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    session.set_keyspace("food")
    session.execute("CREATE TABLE if NOT EXISTS fruit ( id INT PRIMARY KEY, qty text, NAME text);")
    print("Done")


def bulk_transaction(cql):
    bulk_query = ""
    first_command = "BEGIN BATCH"
    last_command = "APPLY BATCH;"
    global cql_transaction
    cql_transaction.append(cql)
    if len(cql_transaction) > 10000:
        for query in cql_transaction:
            query = " " + query
            bulk_query += query
        print(bulk_query)
        try:
            session.execute(first_command + bulk_query + last_command)
        except:
            pass
        cql_transaction = []


def cql_insert(qty, name, id):
    try:
        cql = """INSERT INTO fruit (id, qty, name) VALUES ({},{},'{}') IF NOT EXISTS;""".format(int(id), int(qty), name)
        bulk_transaction(cql)
    except Exception as e:
        print(e)


def get_data(row):
    try:
        qty = row["_source"]["qty"]
    except Exception as e:
        qty = " "

    try:
        name = row["_source"]["name"]
    except Exception as e:
        name = " "

    try:
        id = row["_id"]
    except Exception as e:
        id = " "

    return qty, name, id


if __name__ == "__main__":
    es = connect_elasticsearch()
    data = search_for_esdata(es)
    create_keyspace_table()
    for row in data:
        qty, name, id = get_data(row)
        print(qty,name, id)
        cql_insert(qty, name, id)






