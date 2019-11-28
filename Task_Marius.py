from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML
from rdflib import Graph
import json

input_location = "\"Mannheim\"" + "@de"

# Inital Query

dbpedia = SPARQLWrapper("http://dbpedia.org/sparql")
dbpedia.setQuery("""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    SELECT ?a ?x ?y
    WHERE { 
            {
            ?a rdfs:label %s .
            ?a rdf:type dbo:City .
            ?a geo:lat ?x .
            ?a geo:long ?y
            }
            UNION
            {
            ?a rdfs:label %s .
            ?a rdf:type dbo:Town .
            ?a geo:lat ?x .
            ?a geo:long ?y
            }
            UNION
            { 
            ?a rdfs:label %s .
            ?a rdf:type dbo:Village .
            ?a geo:lat ?x .
            ?a geo:long ?y
            }
          }
""" % (input_location, input_location, input_location))
dbpedia.setReturnFormat(XML)
results = dbpedia.query().convert()
print(results.toxml())


# Air Quality DB

airquality = SPARQLWrapper("http://lod.cs.aau.dk:8891/sparql")

airquality.setQuery("""
    PREFIX schema: <http://qweb.cs.aau.dk/airbase/schema/>
    PREFIX property: <http://qweb.cs.aau.dk/airbase/property/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX dbr: <http://dbpedia.org/resource/>
    SELECT ?station ?long ?lat ?c
    WHERE { 
            ?obs schema:station ?station .
            ?station schema:inCity ?c .
            ?c owl:sameAs ?city
            ?city rdf:type dbo:City .
            ?station property:longitudeDegree ?long .
            ?station property:latitudeDegree ?lat . 
          }
""")

airquality.setReturnFormat(XML)
results = airquality.query().convert()
print("--------------------------------------------")
print("\n AIR QUALITY RESULTS:\n")
print(results.toxml())