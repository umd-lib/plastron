from rdflib import Graph, URIRef

class Resource():

    ''' methods for interacting with fcrepo resources '''

    def __init__(self):
        self.graph = Graph()
        self.uri = URIRef('')
        self.__dict__.update(metadata)
        namespace_manager = NamespaceManager(Graph())


'''        for k, v in ns.items():
            namespace_manager.bind(k, v, override=True, replace=True)
        self.graph.namespace_manager = namespace_manager


    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph.serialize(format="turtle").decode())


    # create an rdfsource container resource
    def create_rdf(self, endpoint, auth):
        headers = {'Content-Type': 'text/turtle'}
        data = self.graph.serialize(format="turtle")
        response = requests.post(endpoint, 
                                 auth=auth, 
                                 data=data,
                                 headers=headers
                                 )
        if response.status_code == 201:
            self.repopath = response.text[len(endpoint):]
            self.uri = REST_ENDPOINT + self.repopath
            return True
        else:
            return False


    # upload a binary resource
    def create_nonrdf(self, endpoint, parentpath, auth):
        int_uri = "{0}{1}/internal".format(endpoint, parentpath)
        print(int_uri)
        data = open(self.local_path, 'rb').read()
        headers = {'Content-Type': 'application/pdf',
                   'Digest': 'sha1={0}'.format(self.checksum),
                   'Content-Disposition': 
                        'attachment; filename="{0}"'.format(self.filename)
                    }
        response = requests.put(int_uri, 
                                 auth=auth, 
                                 data=data, 
                                 headers=headers
                                 )
        if response.status_code == 201:
            self.uri = REST_ENDPOINT + response.text[len(endpoint):]
            return True
        else:
            return False


    # update the subject of graph after URI assignment
    def update_graph(self):
        for (s, p, o) in self.graph:
            self.graph.remove((s, p, o))
            self.graph.add((URIRef(self.uri), p, o))


    # update existing resource with sparql update
    def sparql_update(self, endpoint, auth, triples):
        data = []
        for ns_pre, ns_uri in ns.items():
            data.append('PREFIX {0}: <{1}>'.format(ns_pre, ns_uri))
        data.append('INSERT DATA {')
        for (prefix, predicate, object) in triples:
            data.append('<> {0}:{1} {2} .'.format(prefix, predicate, object))
        data.append('}')
        payload = '\n'.join(data)
        headers = {'Content-Type': 'application/sparql-update'}
        response = requests.patch(endpoint, 
                                  auth=auth, 
                                  data=payload,
                                  headers=headers
                                  )
        if response.status_code == 204:
            return True
        else:
            print(response, response.headers)
            print(response.text)
            return False
    
    
    # replace triples on existing resource
    def put_graph(self, endpoint, auth):
        data = self.graph.serialize(format='turtle')
        headers = {'Content-Type': 'text/turtle'}
        response = requests.put(endpoint, 
                                auth=auth, 
                                data=data,
                                headers=headers
                                )
        if response.status_code == 204:
            return True
        else:
            print(response)
            print(response.headers)
            print(response.text)
            return False

'''


#class Patent(Resource):


'''
    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        
        self.filename = os.path.basename(self.asset_path)
        self.file_metadata = {'title': "Color images of {0}".format(self.title),
                              'pages': self.pages, 
                              'scan_date': self.scan_date,
                              'local_path': self.asset_path
                              }
        self.file = File(self.file_metadata)
        
        self.graph.add(
            (self.uri, ns['dcterms'].title, Literal(self.title))
            )
        self.graph.add(
            (self.uri, ns['pcdm'].memberOf, self.collection)
            )
        self.graph.add(
            (self.uri, ns['uspatent-s'].docNo, Literal(self.patent_number))
            )
        self.graph.add(
            (self.uri, ns['dc'].date, Literal(self.date,
                                                datatype=ns['xsd'].date))
            )
        self.graph.add(
            (self.uri, ns['dc'].subject, Literal(self.large_category))
            )
        self.graph.add(
            (self.uri, ns['bibo'].webpage, URIRef(self.patent_url))
            )
        self.graph.add(
            (self.uri, ns['rdf'].type, ns['pcdm'].Object)
            )
        self.graph.add(
            (self.uri, ns['dc'].subject, Literal(self.uspc))
            )
        self.graph.add(
            (self.uri, ns['dc'].identifier, Literal(self.application_number))
            )
        for inventor in self.inventor.split(';'):
            if inventor is not "":
                self.graph.add(
                    (self.uri, ns['dc'].creator, Literal(inventor))
                    )
        for city in self.city.split(';'):
            if city is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorCity, Literal(city))
                    )
        for state in self.state.split(';'):
            if state is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorState, Literal(state))
                    )
        for country in self.country.split(';'):
            if country is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorCountry, Literal(country))
                    )
                    
                    
    def create_ext(self, endpoint, auth):
        headers = {'Content-Type': ('message/external-body; access-type=URL; '
                   'URL="{0}"'.format(self.image_url))
                   }
        ext_uri = "{0}{1}/external".format(endpoint, self.repopath)
        response = requests.put(ext_uri, 
                                auth=auth, 
                                headers=headers
                                )
        if response.status_code == 201:
            return True
        else:
            return False
'''

class Binary(Resource):

    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        self.filename = os.path.basename(self.local_path)
        self.checksum = self.sha1()
        
        self.graph.add(
            (self.uri, ns['dcterms'].title, Literal(self.title))
            )
        self.graph.add(
            (self.uri, ns['rdf'].type, ns['pcdm'].File)
            )
        self.graph.add(
            (self.uri, ns['bibo'].numPages, Literal(self.pages,
                                                  datatype=ns['xsd'].integer))
            )
        self.graph.add(
            (self.uri, ns['dc'].date, Literal(self.scan_date, 
                                               datatype=ns['xsd'].datetime))
            )
    
    
    # confirm accessibility of a local asset
    def file_exists(self):
        if os.path.isfile(self.local_path):
            return True
        else:
            return False
    
    
    # generate SHA1 checksum on a file
    def sha1(self):
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with open(self.local_path, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()

'''


#class Transaction():

        TRANSACTION SYNTAX:
        ===================
        start:      /fcr:tx
        act:        /tx:{transaction_id}/path/to/resource
        keepalive:  /tx:{transaction_id}/fcr:tx
        commit      /tx:{transaction_id}/fcr:tx/fcr:commit
        rollback    /tx:{transaction_id}/fcr:tx/fcr:rollback '''
'''
    def __init__(self, endpoint, auth):
        response = requests.post('{0}/fcr:tx'.format(endpoint), auth=auth)
        if response.status_code == 201:
            self.auth = auth
            self.uri = response.headers['Location']
            self.id = self.uri[len(endpoint):]
            self.commit_uri = "{0}/fcr:tx/fcr:commit".format(self.uri)
            self.rollback_uri = "{0}/fcr:tx/fcr:rollback".format(self.uri)


    # commit transaction
    def commit(self):
        response = requests.post(self.commit_uri, auth=self.auth)
        if response.status_code == 204:
            return True
        else:
            return False


    # rollback transaction
    def rollback(self):
        response = requests.post(self.rollback_uri, auth=self.auth)
        if response.status_code == 204:
            return True
        else:
            return False

'''
