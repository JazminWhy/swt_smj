import pandas as pd
import io
import requests
import matplotlib.pyplot as plt
import base64
import rdflib
import numpy as np
import re

DBPEDIA_ENDPOINT = 'http://dbpedia.org/sparql/'
OPENEI_ENPOINT = 'https://openei.org/sparql/l'
QBOAIRBASE_ENPOINT = 'http://lod.cs.aau.dk:8891/sparql'

DBPEDIA_GET_CITY_COUNTRY = """
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbr: <http://dbpedia.org/resource/>
SELECT ?city ?country WHERE {
 ?city a dbo:PopulatedPlace ;
    rdfs:label "%(label)s"@en ;
    dbo:country ?country
}"""

DBPEDIA_GET_CITY_COUNTRY_STRIPPED = """ PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbr: <http://dbpedia.org/resource/>
SELECT DISTINCT ?city ?country ?strippedLabel WHERE {
 ?city a dbo:PopulatedPlace  ;
    rdfs:label "%(label)s"@en ;
    dbo:country ?country.
 ?country rdfs:label ?countryLabel.
FILTER langMatches( lang(?countryLabel), "EN" ) 
BIND (STR(?countryLabel)  AS ?strippedLabel) 
}"""

OPEN_EI_QUERY_ALL_REGIONS = """ PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
PREFIX dbo: <http://dbpedia.org/ontology>
PREFIX dbr: <http://dbpedia.org/resource>

SELECT DISTINCT(?region) 
FROM <http://openei.org>
WHERE {
 ?organization property:Region ?region.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
}"""

OPEN_EI_QUERY_SPECIFIC_REGION_BY_COUNTRY_FILTER = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
PREFIX dbo: <http://dbpedia.org/ontology>
PREFIX dbr: <http://dbpedia.org/resource>

SELECT DISTINCT(?region) 
FROM <http://openei.org>
WHERE {
 ?organization property:Region ?region.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
FILTER (regex(?region, "%(country)s"))
}


"""

OPEN_EI_QUERY_REGION_BY_OWL_SAME_AS_DBPEDIA = """ PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
PREFIX dbo: <http://dbpedia.org/ontology>
PREFIX dbr: <http://dbpedia.org/resource>

SELECT DISTINCT ?region
FROM <http://openei.org>
WHERE {
?region owl:sameAs <http://dbpedia.org/resource/Cologne> .
}
"""

OPEN_EI_QUERY_ALL_COUNTRYS_IN_A_COUNTRY_WHERE_REGION_EXISTS = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
PREFIX dbo: <http://dbpedia.org/ontology>
PREFIX dbr: <http://dbpedia.org/resource>
SELECT ?page ?name ?address ?place_name ?zip ?coordinates ?sector_name ?category_name
FROM <http://openei.org>
WHERE {
  ?organization swivt:page ?page.
  ?organization rdf:type category:Companies.
  ?organization rdfs:label ?name.
  ?organization property:Region <http://openei.org/resources/Germany> .
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL { ?organization property:Zip ?zip. }.
  OPTIONAL { ?organization property:Coordinates ?coordinates. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
  OPTIONAL {
    ?organization property:Sector ?sector. 
    ?sector rdfs:label ?sector_name.
  }.
  OPTIONAL {
    ?organization rdf:type ?category.
    ?category rdfs:label ?category_name.
  }.
}

 """

OPEN_EI_QUERY_RETRIEVE_ALL_INSTITUTES_FROM_COUNTRY_BY_PLACE_NAME = """ 

PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
SELECT ?page ?name ?address ?place_name ?zip ?coordinates ?sector_name ?category_name
FROM <http://openei.org>
WHERE {
  ?organization swivt:page ?page.
  ?organization rdf:type category:Research_Institutions.
  ?organization rdfs:label ?name.
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL { ?organization property:Zip ?zip. }.
  OPTIONAL { ?organization property:Coordinates ?coordinates. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
  OPTIONAL {
    ?organization property:Sector ?sector. 
    ?sector rdfs:label ?sector_name.
  }.
  OPTIONAL {
    ?organization rdf:type ?category.
    ?category rdfs:label ?category_name.
  }.
FILTER (regex(?place_name, "Germany"))
}

"""

OPEN_EI_QUERY_FINANCIAL_INSTITUTIONS = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
SELECT distinct ?page ?name ?address ?place_name 
FROM <http://openei.org>
WHERE {
  ?organization swivt:page ?page.
  ?organization rdf:type category:Financial_Organizations.
  ?organization rdfs:label ?name.
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
FILTER(regex(?place_name, "%(country)s"))
}

"""

# finds 975 orgas

OPEN_EI_QUERY_ALL_ORGANIZATIONS_FROM_COUNTRY_OR_CITY = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
PREFIX place: <http://openei.org/resources/Place-3A>

SELECT DISTINCT ?organization ?name ?place {
?organization rdf:type category:Organizations.
?organization rdfs:label ?name.
?organization property:Place ?place.
FILTER(regex(?place, "%(country)s"))
}

"""

OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
SELECT DISTINCT ?page ?name ?address ?place_name 
FROM <http://openei.org>
WHERE {
  ?organization swivt:page ?page.
  ?organization rdf:type category:Policy_Organizations.
  ?organization rdfs:label ?name.
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
  OPTIONAL {
    ?organization property:Sector ?sector. 
    ?sector rdfs:label ?sector_name.
  }.
FILTER(regex(?place_name, "%(country)s"))
}
"""

OPEN_EI_QUERIES_FIND_TOOLS = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
SELECT DISTINCT(?organization) ?name ?address ?place_name 
FROM <http://openei.org>
WHERE {
  ?organization rdf:type category:Tools.
  ?organization rdfs:label ?name.
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL { ?organization property:Zip ?zip. }.
  OPTIONAL { ?organization property:Coordinates ?coordinates. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
  OPTIONAL {
    ?organization property:Sector ?sector. 
    ?sector rdfs:label ?sector_name.
  }.
  OPTIONAL {
    ?organization rdf:type ?category.
    ?category rdfs:label ?category_name.
  }.
FILTER(regex(?place_name, "%(country)s"))
}

"""

OPEN_EI_CLEAN_ENERGY_COMPANIES = """ 
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX swivt: <http://semantic-mediawiki.org/swivt/1.0#>
PREFIX resource: <http://openei.org/resources/>
PREFIX category: <http://openei.org/resources/Category-3A>
PREFIX property: <http://openei.org/resources/Property-3A>
SELECT DISTINCT(?organization) ?name ?address ?place_name 
FROM <http://openei.org>
WHERE {
  ?organization rdf:type category:Companies.
  ?organization rdfs:label ?name.
  OPTIONAL { ?organization property:Address ?address. }.
  OPTIONAL { ?organization property:Zip ?zip. }.
  OPTIONAL { ?organization property:Coordinates ?coordinates. }.
  OPTIONAL {
    ?organization property:Place ?place.
    ?place rdfs:label ?place_name.
  }.
  OPTIONAL {
    ?organization property:Sector ?sector. 
    ?sector rdfs:label ?sector_name.
  }.
  OPTIONAL {
    ?organization rdf:type ?category.
    ?category rdfs:label ?category_name.
  }.
FILTER(regex(?place_name, "%(country)s"))
}

"""

OPEN_EI_CLEAN_ENERGY_COMPANIES_PARAMS = ['organization', 'name', 'place_name']
OPEN_EI_ORGANIZATIONS_PARAMS = ['organization', 'name', 'place']
OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY_PARAMS = ['page', 'name', 'address', 'place_name']
OPEN_EI_ORGANIZATION_PARAMS = ['page', 'name', 'place_name', 'address']
OPEN_EI_FINANCIAL_PARAMS = ['page', 'name', 'address', 'place_name']


# g_facility = rdflib.Graph()
# g_facility.parse("static/Facility.rdf")

def run_query_facility(dataset, query):
    r = dataset.query(query)
    print('Result of EEA query')
    for row in r.result:
        # print(row)
        yield (row[0], row[1], row[2])


def run_query(endpoint, query, vars):
    r = requests.get(endpoint,
                     params={'query': query, 'format': 'application/sparql-results+json', 'charset': 'utf-8'})
    # print(r.content)
    for row in r.json()['results']['bindings']:
        yield [row[v]['value'] for v in vars]


def get_airquality(name):
    results = pd.DataFrame(columns=['Station', 'Longitude', 'Latitude', 'CO', 'Time', 'Tech'])

    query_dbpedia = """
        SELECT ?city {
        ?city rdf:type dbo:PopulatedPlace .
        ?city rdfs:label "%(label)s"@de . 
        }""" % {'label': name}

    print(query_dbpedia)
    for city, in run_query(DBPEDIA_ENDPOINT, query_dbpedia, ['city']):

        query_airbase = """
        PREFIX property: <http://qweb.cs.aau.dk/airbase/property/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX schema: <http://qweb.cs.aau.dk/airbase/schema/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        
        SELECT DISTINCT ?time ?stationName ?long ?lat AVG(?s) as ?s
        
        WHERE { 
            ?obs schema:station ?station .
            ?station schema:inCity ?c;
            property:longitudeDegree ?long;
            property:latitudeDegree ?lat ;
            property:station ?stationName .
            ?obs schema:sensor ?sensor .
            ?sensor property:statisticShortName "Mean"^^xsd:string .
            ?sensor property:automaticMeasurement ?tech .
            ?obs schema:year ?year .
            ?year property:yearNum ?time .
            ?obs schema:CO ?s .
            ?c owl:sameAs <%(city)s> .
        }""" % {'city': city}
        print(query_airbase)
        for stationName, long, lat, s, time in run_query(QBOAIRBASE_ENPOINT, query_airbase,
                                                         ['stationName', 'long', 'lat', 's', 'time']):
            results = results.append(
                {'Station': stationName, 'Longitude': long, 'Latitude': lat, 'CO': s, 'Time': time},
                ignore_index=True)
    results = results.sort_values(["Station", "Time"])
    return results


def get_environmental_facilities(name):
    queryDbpedia = """
    PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbo:<http://dbpedia.org/ontology/>

    SELECT DISTINCT ?city {
    ?city rdfs:label "%(label)s"@de .
    ?city a dbo:PopulatedPlace .
    }""" % {'label': name}

    query_EEA = """
    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX sites:<http://eunis.eea.europa.eu/rdf/sites-schema.rdf#>
    PREFIX facility:<http://prtr.ec.europa.eu/rdf/schema.rdf#>

    SELECT ?name ?lat ?long
       WHERE {
       ?facility facility:facilityName ?name .
       ?facility facility:inCountry ?country .
       ?facility facility:city ?place_name .
       ?facility geo:lat ?lat .
       ?facility geo:long ?long .

    FILTER ( regex(?place_name, "%(city1)s" ) )
    }""" % {'city1': name}

    try:
        if run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city']).__next__():
            print('query result not none')

        for city, in run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city']):
            print('city is .. {}'.format(city))

            result = pd.DataFrame()
            for facility, lat, long in run_query_facility(g_facility, query_EEA):
                # print("\t", facility, lat, long)
                result = result.append({'Facility': facility, 'Longitude': long, 'Latitude': lat}, ignore_index=True)
    except Exception as exp:
        print(exp)
        result = pd.DataFrame()
        for facility, lat, long in run_query_facility(g_facility, query_EEA):
            # print("\t", facility, lat, long)
            result = result.append({'Facility': facility, 'Longitude': long, 'Latitude': lat}, ignore_index=True)

    return result


def run_tools_by_country(name):
    results = pd.DataFrame()
    queryDbpedia = DBPEDIA_GET_CITY_COUNTRY_STRIPPED % {'label': name}
    for city, country, strippedLabel in run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city', 'country', 'strippedLabel']):
        query_OpenEi = OPEN_EI_QUERIES_FIND_TOOLS % {'country': name}

        for a, b, c in run_query(OPENEI_ENPOINT, query_OpenEi, OPEN_EI_CLEAN_ENERGY_COMPANIES_PARAMS):
            results = results.append(pd.Series({'organization': a, 'name': b, 'place_name': c}),
                                     ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)

    print("Found {} tools in {}".format(len(results), name))
    results = pd.DataFrame()

    query_OpenEi = OPEN_EI_QUERIES_FIND_TOOLS % {'country': strippedLabel}

    for a, b, c in run_query(OPENEI_ENPOINT, query_OpenEi, OPEN_EI_CLEAN_ENERGY_COMPANIES_PARAMS):
        results = results.append(pd.Series({'organization': a, 'name': b, 'place_name': c}),
                                 ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)
    print("In total found {} tools in {}".format(len(results), strippedLabel))
    results['organization'] = results['organization'].apply(
        lambda x: re.sub('http://openei.org/resources', 'https://openei.org/wiki', x))
    return results


def run_clean_energy_company_by_country(name):
    results = pd.DataFrame()
    queryDbpedia = DBPEDIA_GET_CITY_COUNTRY_STRIPPED % {'label': name}

    for city, country, strippedLabel in run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city', 'country', 'strippedLabel']):
        query_OpenEi = OPEN_EI_CLEAN_ENERGY_COMPANIES % {'country': name}

        for a, b, c in run_query(OPENEI_ENPOINT, query_OpenEi, OPEN_EI_CLEAN_ENERGY_COMPANIES_PARAMS):
            results = results.append(pd.Series({'organization': a, 'name': b, 'place_name': c}),
                                     ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)

    print("Found {} clean energy organizations in {}".format(len(results), name))
    results = pd.DataFrame()

    query_OpenEi = OPEN_EI_CLEAN_ENERGY_COMPANIES % {'country': strippedLabel}

    for a, b, c in run_query(OPENEI_ENPOINT, query_OpenEi, OPEN_EI_CLEAN_ENERGY_COMPANIES_PARAMS):
        results = results.append(pd.Series({'organization': a, 'name': b, 'place_name': c}),
                                 ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)
    print("In total found {} clean energery organizations in {}".format(len(results), strippedLabel))
    return results


def run_policy_by_country(name):
    results = pd.DataFrame()
    queryDbpedia = DBPEDIA_GET_CITY_COUNTRY_STRIPPED % {'label': name}

    for city, country, strippedLabel in run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city', 'country', 'strippedLabel']):
        query_OpenEi = OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY % {'country': name}

        for a, b, c, d in run_query(OPENEI_ENPOINT, query_OpenEi,
                                    OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY_PARAMS):
            results = results.append(pd.Series({'page': a, 'name': b, 'address': c, 'place_name': d}),
                                     ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)
    print("Found {} policy organizations in {}".format(len(results), name))
    results = pd.DataFrame()

    for city, country, strippedLabel in run_query(DBPEDIA_ENDPOINT, queryDbpedia, ['city', 'country', 'strippedLabel']):
        query_OpenEi = OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY % {'country': strippedLabel}

        for a, b, c, d in run_query(OPENEI_ENPOINT, query_OpenEi,
                                    OPEN_EI_QUERY_POLICY_ORGANIZATIONS_PER_COUNTRY_PARAMS):
            results = results.append(pd.Series({'page': a, 'name': b, 'address': c, 'place_name': d}),
                                     ignore_index=True)  # results.append(pd.Series(data=[a,b,c]), ignore_index=True)

    print("In total found {} policy organization in {}".format(len(results), strippedLabel))
    return results


def build_airquality_graphs(data):
    data = data.sort_values(['Station','Time'])
    results = []
    for station in data['Station'].unique():
        print(station)
        temp_data = data[data.Station == station]
        img = io.BytesIO()
        x = temp_data['Time'].astype(int)
        y = temp_data['CO'].astype(float).round(decimals=5)

        plt.title("Airquality results " + station + " from " + str(x.min()) + " until " + str(x.max()))
        plt.yticks((np.arange(0,1, step=0.1)))
        plt.plot(x, y)
        plt.savefig(img, format='png')
        img.seek(0)
        png = base64.b64encode(img.getvalue()).decode('ascii')
        results.append(png)
        plt.close()
    return results


if __name__ == "__main__":
    build_airquality_graphs(get_airquality('Berlin'))
