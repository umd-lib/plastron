from io import BytesIO
import pprint
import requests
import rdflib
from rdflib import Namespace

#============================================================================
# PCDM RESOURCE
#============================================================================

class Resource():

    def __init__(self):
        self.graph = rdflib.Graph()


    def get_uri(self, endpoint, user, password):
        response = requests.post(endpoint, auth = (user, password))
        if response.status_code == 201:
            self.uri = response.text
            return True
        else:
            return False


    def create_graph(self):
        print('Building resource graph...')
        s = self.uri
        for (p,o) in self.metadata:
            self.graph.add( (s, p, o) )


    def add_metadata_to_graph(self, dryrun=True):
        if not hasattr(self, 'uri'):
            uri = self.path
        s = rdflib.URIRef(self.uri)
        for b,p,o in self.metadata:
            self.graph.add( (s,p,o) )


    def deposit(self, user, password):
        query = ["PREFIX dc: <http://purl.org/dc/elements/1.1/>",
                 "PREFIX bibo: <http://purl.org/ontology/bibo/>",
                 "PREFIX foaf: <http://xmlns.com/foaf/0.1/>",
                 "INSERT {"]
        for (s,p,o) in self.graph:
            query.append("<> <{0}> '{1}' ;".format(p,o))
        query[-1] = query[-1].rstrip(';') + '.'
        query.append("}")
        
        print("\n".join(query))
        
        data = '\n'.join(query).encode('utf-8')

        headers = {'Content-Type': 'application/sparql-update'}
        response = requests.patch(  self.uri, 
                                    data=data, 
                                    auth=(user, password),
                                    headers=headers
                                    )
        return response


#============================================================================
# PCDM ITEM-OBJECT
#============================================================================

class ItemObj(Resource):

    def __init__(self, data):
        Resource.__init__(self)
        self.graph = rdflib.Graph()
        self.components = [CompObj(p) for p in data.pages]
        self.path = data.path
        self.metadata = data.metadata


#============================================================================
# PCDM COMPONENT-OBJECT
#============================================================================

class CompObj(Resource):

    def __init__(self, data):
        Resource.__init__(self)
        self.graph = rdflib.Graph()
        self.files = [FileObj(f) for f in data.files]


#============================================================================
# PCDM FILE
#============================================================================

class FileObj(Resource):

    def __init__(self, data):
        Resource.__init__(self)


#============================================================================
# PCDM PROXY OBJECT
#============================================================================

class ProxyObj(Resource):
    
    def __init__(self):
        Resource.__init__(self)


