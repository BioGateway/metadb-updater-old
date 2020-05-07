import urllib.parse


def generate_field_query(graph, relationType, constraint, count=False):
    select = "COUNT(?uri)" if count else "?uri ?value"
    query = """
    SELECT %s
    WHERE {
    GRAPH <http://rdf.biogateway.eu/graph/%s> {
        ?uri %s ?value .
        %s
    }
    }
    """ % (select, graph, relationType, constraint)
    return query

def generate_name_label_query(graph, constraint, count=False):
    select = "COUNT(?uri)" if count else "?uri ?prefLabel ?definition"
    query = """
    SELECT DISTINCT %s
    WHERE {
    GRAPH <http://rdf.biogateway.eu/graph/%s> {
    ?uri skos:prefLabel|rdfs:label ?prefLabel .
    %s
    OPTIONAL { ?uri skos:definition ?definition . }
    }}
    """ % (select, graph, constraint)
    return query

def generate_fromScore_query(graph, constraint):
    query = """
    SELECT DISTINCT ?uri COUNT(?node) as ?relationCount
    WHERE {
    GRAPH <http://rdf.biogateway.eu/graph/%s> {
    ?uri skos:prefLabel|rdfs:label ?label .
    %s
    }
    GRAPH ?graph {
    ?uri ?relation ?node .
    }
    }
    """ % (graph, constraint)
    return query

def generate_toScore_query(graph, constraint):
    query = """
    SELECT DISTINCT ?uri COUNT(?node) as ?relationCount
    WHERE {
    GRAPH <http://rdf.biogateway.eu/graph/%s> {
    ?uri skos:prefLabel|rdfs:label ?label .
    %s
    }
    GRAPH ?graph {
    ?node ?relation ?uri .
    }
    }
    """ % (graph, constraint)
    return query

def generate_scores_query(graph, constraint):
    query = """
    SELECT DISTINCT ?uri ?fromScore ?toScore
    WHERE {
    GRAPH <http://rdf.biogateway.eu/graph/%s> {
    ?uri skos:prefLabel|rdfs:label ?label .
    %s
    }
	{
	SELECT ?uri COUNT(?fromNode) as ?fromScore
	WHERE {
    GRAPH ?fromGraph {
    ?fromNode ?relation ?uri .
    }}}
	{
	SELECT ?uri COUNT(?toNode) as ?toScore
	WHERE {
    GRAPH ?toGraph {
    ?uri ?relation ?toNode .
    }}}
	}
    """ % (graph, constraint)
    return query

def generate_GO_namespace_constraint(namespace):
    constraint = "?uri <http://www.geneontology.org/formats/oboInOwl#hasOBONamespace> \""\
                 + namespace + "\" ^^<http://www.w3.org/2001/XMLSchema#string> ."
    return constraint

def generateUrl(baseUrl, query, testing):
    if testing:
        query += "\nLIMIT 10000"
    return "http://" + baseUrl + "/sparql/" + "?query=" + urllib.parse.quote(query) + "&format=text%2Ftab-separated-values&timeout=0"