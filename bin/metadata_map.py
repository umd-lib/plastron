# MAPPING FOR METADATA ELEMENTS FROM NDNP XML

batch = {
    'issues':   "./{http://www.loc.gov/ndnp}issue"
    }


issue = {
    'volume':   (".//{http://www.loc.gov/mods/v3}detail[@type='volume']/"
                "{http://www.loc.gov/mods/v3}number"
                ),
    'issue':    (".//{http://www.loc.gov/mods/v3}detail[@type='issue']/"
                "{http://www.loc.gov/mods/v3}number"
                ),
    'edition':  (".//{http://www.loc.gov/mods/v3}detail[@type='edition']/"
                "{http://www.loc.gov/mods/v3}number"
                ),
    'date':     (".//{http://www.loc.gov/mods/v3}dateIssued"
                ),
    'lccn':     (".//{http://www.loc.gov/mods/v3}"
                "identifier[@type='lccn']"
                ),
    'pages':    (".//{http://www.loc.gov/METS/}dmdSec"
                ),
    'files':    (".//{http://www.loc.gov/METS/}fileGrp"
                )
    }


page = {
    'number':   (".//{http://www.loc.gov/mods/v3}start"
                ),
    'reel':     (".//{http://www.loc.gov/mods/v3}"
                "identifier[@type='reel number']"
                ),
    'location': (".//{http://www.loc.gov/mods/v3}physicalLocation"
                ),
    'frame':    (".//{http://www.loc.gov/mods/v3}"
                "identifier[@type='reel sequence number']"
                ),
    'files':    (".//{http://www.loc.gov/METS/}file"
                )
    }
    
    
file = {
    'number':   (".//{http://www.loc.gov/mods/v3}start"
                ),
    }

