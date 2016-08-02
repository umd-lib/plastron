#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''On LDP, see http://www.w3.org/TR/2015/REC-ldp-20150226'''

import os
import rdflib
import requests
import sys


class Resource():
    '''Class representing a Linked Data Platform Resource (LDPR)
    A HTTP resource whose state is represented in any way that conforms to the 
    simple lifecycle patterns and conventions in section 4. Linked Data Platform 
    Resources.'''
    def __init__(self):
        pass
    
class RdfSource(Resource):
    '''Class representing a Linked Data Platform RDF Source (LDP-RS)
    An LDPR whose state is fully represented in RDF, corresponding to an RDF 
    graph. See also the term RDF Source from [rdf11-concepts].'''
    def __init__(self):
        pass

class NonRdfSource(Resource):
    '''Class representing a Linked Data Platform Non-RDF Source (LDP-NR)
    An LDPR whose state is not represented in RDF. For example, these can be
    binary or text documents that do not have useful RDF representations.'''
    def __init__(self):
        pass

class Container(RdfSource):
    '''Class representing a Linked Data Platform Container (LDPC)
    A LDP-RS representing a collection of linked documents (RDF Document 
    [rdf11-concepts] or information resources [WEBARCH]) that responds to client 
    requests for creation, modification, and/or enumeration of its linked 
    members and documents, and that conforms to the simple lifecycle patterns 
    and conventions in section 5. Linked Data Platform Containers.'''
    def __init__(self):
        pass

class BasicContainer(Container):
    '''Class representing a Linked Data Platform Basic Container (LDP-BC)
    An LDPC that defines a simple link to its contained documents (information 
    resources) [WEBARCH].'''
    def __init__(self):
        pass

class DirectContainer(Container):
    '''Class representing a Linked Data Platform Direct Container (LDP-DC)
    An LDPC that adds the concept of membership, allowing the flexibility of 
    choosing what form its membership triples take, and allows members to be any 
    resources [WEBARCH], not only documents.'''
    def __init__(self):
        pass
    
class IndirectContainer(Container):
    '''Class representing a Linked Data Platform Indirect Container (LDP-IC)
    An LDPC similar to a LDP-DC that is also capable of having members whose 
    URIs are based on the content of its contained documents rather than the     
    URIs assigned to those documents.'''
    def __init__(self):
        pass
