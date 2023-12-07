from datetime import datetime
from elasticsearch import Elasticsearch
import locale


locale.setlocale(locale.LC_ALL, '')
locale._override_localeconv = {'mon_thousands_sep': ' '}

es = Elasticsearch([{"scheme": "http", "host": "localhost", "port": 9200}], timeout=1.0)
if not es.ping():
    print("Elastic connection failed.")
    exit()

print(f"ElasticSearch v{es.info().get('version').get('number')}")


def methods():
    for attrib in filter(lambda x: not x.startswith("_"), dir(es)):
        print(f"{attrib:40}", end="")
    exit()


# methods()


def liste_index():
    print("Liste des index :")
    # for idx in filter(lambda x: not x.startswith("."), es.indices.get(index="*").keys()):
    for idx in es.indices.get(index="*").keys():
        print("  -", idx, end=" ")
        print(f"({es.cat.count(index=idx).body.split()[2]} enregs)")
        for mapping in es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"]:
            print("     >", mapping, end=" : ")
            print(es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"].get(mapping).get("type"))
        print()

    print()
    exit()


# liste_index()


def aggregations():
    # query = {"match": {"age": 36}}
    # query = {"bool": {"must": [{"match": {"age": 36}}]}}
    # query = {"bool": {"should": [{"match": {"age": 36}}]}}
    # query = {"range": {"age": {"lte": 36}}}
    aggs = {
        "max_number": {
          "terms": {
            "field": "date",
            "format": "yyyy-MM-dd",
            "size": 1,
            "order": {
              "_key": "desc"
            }
          },
          "aggs": {
            "top_hits": {
              "top_hits": {
                "size": 30
              }
            }
          }
        }
      }

    # result = es.search(index="bank", query={"match": {"age": 36}})
    # result = es.search(index="bank", query=query)
    result = es.search(index="suivi-activite", aggs=aggs)

    # aggregations.max_number.buckets.top_hits.hits.hits.fields.total_individus
    total: int = 0
    liste: str = ""
    for doc in result["aggregations"]["max_number"]["buckets"]:  # .get("hits").get("hits"):
        print("date =", doc["top_hits"]["hits"]["hits"][0]["_source"]["date"])
        for elt in doc["top_hits"]["hits"]["hits"]:
            total += elt["_source"]["total_individus"]
            liste += elt["_source"]["code_partenaire"] + ", "

    print("codes partenaires =", liste[:-2])
    print("Total individus = ", end="")
    print(locale.format_string('%.d', total, grouping=True, monetary=True))
    exit()


aggregations()


def add_doc():
    doc = {
        'author': 'christophe Jacques',
        'text': 'At the beginning, is a very delicate time',
        'timestamp': datetime.now(),
    }
    # resp = es.index(index="new-famille", id="1", document=doc)
    resp = es.index(index="new-famille", document=doc)
    print(resp['result'])
    print(resp)
