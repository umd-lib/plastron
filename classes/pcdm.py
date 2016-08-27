import pprint
import requests
import rdflib
from rdflib import Namespace

#============================================================================
# CLASSES
#============================================================================

class Resource():

    def __init__(self):
        pass

    def getUri(self, endpoint, user, password):
        response = requests.post(endpoint, auth = (user, password))
        if response.status_code == 201:
            self.uri = response.text
            return True
        else:
            return False

    def createGraph(self):
    
        print('Building resource graph...')
        
        # Set the URI that identifies this resource
        if not hasattr(self, 'uri'):
            print('  => Getting URI from fcrepo...', end='')
            if getUri(self):
                print('success!')
            else:
                print('ERROR: Failed to create fcrepo container!')
        else:
            print('  => using existing URI...')
            
        print('  => resource URI: {0}'.format(self.uri))
        
        # Create the graph
        g = rdflib.Graph()
        s = rdflib.term.URIRef(self.uri)
        for k, v in self.metadata.items():
            p = rdflib.term.URIRef(metadata_map[k])
            o = rdflib.Literal(v)
            g.add( (s, p, o) )
        
        for statement in g:
            pprint.pprint(statement)

