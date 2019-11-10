from SPARQLWrapper import SPARQLWrapper, JSON, N3, XML
from rdflib import Graph
import json

input_location = "\"Frankfurt\"" + "@en"

# Inital Query

sparql = SPARQLWrapper("http://dbpedia.org/sparql")
sparql.setQuery("""
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
sparql.setReturnFormat(XML)
results = sparql.query().convert()
print(results.toxml())
