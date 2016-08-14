import pprint
import requests
import rdflib
from rdflib import Namespace

#============================================================================
# METADATA NAMESPACES AND PREDICATES
#============================================================================

pcdm = {
    'pcdm:AdministrativeSet':   'http://pcdm.org/models#AdministrativeSet',
    'pcdm:AlternateOrder':      'http://pcdm.org/models#AlternateOrder',
    'pcdm:Collection':          'http://pcdm.org/models#Collection',
    'pcdm:File':                'http://pcdm.org/models#File',
    'pcdm:Object':              'http://pcdm.org/models#Object',
    'pcdm:fileOf':              'http://pcdm.org/models#fileOf',
    'pcdm:hasFile':             'http://pcdm.org/models#hasFile',
    'pcdm:hasMember':           'http://pcdm.org/models#hasMember',
    'pcdm:hasRelatedObject':    'http://pcdm.org/models#hasRelatedObject',
    'pcdm:memberOf':            'http://pcdm.org/models#memberOf',
    'pcdm:relatedObjectOf':     'http://pcdm.org/models#relatedObjectOf'
    }


metadata_map = {
    'title': 'dcterms:title',
    'date': 'dcterms:issued',
    'creator': 'dcterms:creator',
    'lccn': 'dc:identifier@type:lccn',
    'volume': 'bibo:volume',
    'issue': 'bibo:issue',
    'edition': 'bibo:edition',
    'page': 'bibo:pageStart',
    }


premis =          Namespace('<http://www.loc.gov/premis/rdf/v1#>')
image =           Namespace('<http://www.modeshape.org/images/1.0>')
sv =              Namespace('<http://www.jcp.org/jcr/sv/1.0>')
nt =              Namespace('<http://www.jcp.org/jcr/nt/1.0>')
rdfs =            Namespace('<http://www.w3.org/2000/01/rdf-schema#>')
xsi =             Namespace('<http://www.w3.org/2001/XMLSchema-instance>')
mode =            Namespace('<http://www.modeshape.org/1.0>')
xmlns =           Namespace('<http://www.w3.org/2000/xmlns/>')
rdf =             Namespace('<http://www.w3.org/1999/02/22-rdf-syntax-ns#>')
xml =             Namespace('<http://www.w3.org/XML/1998/namespace>')
jcr =             Namespace('<http://www.jcp.org/jcr/1.0>')
ebucore =         Namespace(
                    '<http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#>'
                    )
ldp =             Namespace('<http://www.w3.org/ns/ldp#>')
xs =              Namespace('<http://www.w3.org/2001/XMLSchema>')
mix =             Namespace('<http://www.jcp.org/jcr/mix/1.0>')
prov =            Namespace('<http://www.w3.org/ns/prov#>')
foaf =            Namespace('<http://xmlns.com/foaf/0.1/>')
dc =              Namespace('<http://purl.org/dc/elements/1.1/>')
bibo =            Namespace('http://purl.org/ontology/bibo/')


#============================================================================
# CLASSES
#============================================================================

class Container():

    '''A class representing a digital asset that can be understood as a 
       self-cotained unit of description (for example a book, film, audio
       recording, etc.'''

    def __init__(self, inputdata, **kwargs):
        self.metadata = inputdata['metadata']
        self.components = [Container(c) for c in inputdata['components']]
        self.files = [File(f) for f in inputdata['files']]


    def __str__(self):
        s = "\n".join(
            ["{0}: {1}".format(k,v) for k,v in self.metadata.items()]
            )
        return s


    def getUri(self):
        response = requests.post(REST_ENDPOINT, 
                                auth = (FEDORA_USER, FEDORA_PASSWORD)
                                )
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



class Component(Container):

    '''A class representing a digital asset that can be understood as a 
       self-cotained unit of access or representation (for example a page, 
       a 'side' of a recording, or a part of a multi-part work.'''

    def __init__(self, inputdata):
        pass
    


class File(Container):

    def __init__(self, path):
        pass
