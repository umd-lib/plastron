#!/usr/bin/env python3

import os
import rdflib

from test_data import testdata


#============================================================================
# CLASSES
#============================================================================

class PcdmItem():

    def __init__(self, inputdata, **kwargs):
        
        metadata = inputdata['metadata']
        for field in metadata: setattr(self, field, metadata[field])
        for k in kwargs: setattr(self, k, kwargs[k])
        
        self.components = []
        self.files = []
        self.components = [PcdmComponent(c) for c in inputdata['components']]
        self.files = [PcdmFile(f) for f in inputdata['files']]
        


class PcdmComponent():

    def __init__(self, inputdata):
        pass
    


class PcdmFile():

    def __init__(self, inputdata):
        pass



#============================================================================
# MAIN LOOP
#============================================================================

def main():
    
    myitem = PcdmItem(testdata, foo='bar')
    
    
    
    
    properties = (vars(myitem))
    for k,v in properties.items():
        print("{0} : {1}".format(k,v))
    

if __name__ == "__main__":
    main()
