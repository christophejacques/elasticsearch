from datetime import datetime
from elasticsearch import Elasticsearch
import locale


locale.setlocale(locale.LC_ALL, '')
locale._override_localeconv = {'mon_thousands_sep': ' '}


class Elastic:

    def __init__(self, host: str, port: int):
        self.es = Elasticsearch([{"scheme": "http", "host": host, "port": port}], request_timeout=10.0)
        if not self.es.ping():
            print("Elastic connection failed.")
            exit()

        print(f"ElasticSearch v{self.es.info().get('version').get('number')} connected")
        self.index: str = ""

    def close(self):
        self.es.close()

    def use_index(self, index_name: str):
        if index_name not in self.es.indices.get(index=index_name).keys():
            raise Exception(f"L'index '{index_name}' n'existe pas.")

        self.index = index_name

    def get_mapping(self) -> list:
        liste: list = list()
        for mapping in self.es.indices.get_mapping(index=self.index)[self.index]["mappings"]["properties"]:
            liste.append(mapping)

        return liste

    def liste_index(self):
        print("Liste des index :")
        # for idx in filter(lambda x: not x.startswith("."), es.indices.get(index="*").keys()):
        for idx in self.es.indices.get(index="*").keys():
            print("  -", idx, end=" ")
            print(f"({self.es.cat.count(index=idx).body.split()[2]} enregs)")
            for mapping in self.es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"]:
                print("     >", mapping, end=" : ")
                print(self.es.indices.get_mapping(index=idx)[idx]["mappings"]["properties"].get(mapping).get("type"))
            print()

        print()
        exit()

    def search(self, index: str, query: str, size: int = 10):
        properties = es.es.search(index=index, q=query, size=size).get("hits")
        return properties

    def delete_index(self, index_name: str):
        self.es.delete_by_query(index=index_name, q="_id: *")

    def aggregations(self):
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
        result = self.es.search(index="suivi-activite", aggs=aggs)

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

    def add_doc(self, document):
        resp = self.es.index(index=self.index, document=document)
        print(resp['result'], flush=True)


if __name__ == "__main__":
    es = Elastic("localhost", 9200)
    es.use_index("suivi-activite")
    mapping: list = es.get_mapping()
    
    es.delete_index("creation")
    es.use_index("creation")

    properties = es.search(index="suivi-activite", query="_id:*", size=10)
    nb_docs = properties["total"].get("value")
    print("Nb docs:", nb_docs)

    num: int = 0
    for docs in properties.get("hits"):
        document: dict = {}
        num += 1
        print(f"{num:3}", docs.get("_id"), ": ", end="")
        for cle in mapping:
            document[cle] = docs["_source"].get(cle)
            print(document[cle], end=" / ")

        es.add_doc(document)
