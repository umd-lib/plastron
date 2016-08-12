#!/usr/bin/env python3

testdata = {
    'metadata': {
        'title': 'Diamondback',
        'volume': 1,
        'issue': 1,
        'edition': 1
        },
    'components': [
            {
            'metadata': {
                'order': 1, 
                'path': 'path/to/page/1/'
                },
            'components': [],
            'files': [
                {'use': 'master',       'path': 'path/to/page/1/page1.jp2'},
                {'use': 'derivative',   'path': 'path/to/page/1/page1.jpg'},
                {'use': 'embedded',     'path': 'path/to/page/1/page1.pdf'},
                {'use': 'ocr',          'path': 'path/to/page/1/page1.xml'}
                ]
            },{
            'metadata': {
                'order': 2,
                'path': 'path/to/page/2/'
                },
            'components': [],
            'files': [
                {'use': 'master',       'path': 'path/to/page/2/page2.jp2'},
                {'use': 'derivative',   'path': 'path/to/page/2/page2.jpg'},
                {'use': 'embedded',     'path': 'path/to/page/2/page2.pdf'},
                {'use': 'ocr',          'path': 'path/to/page/2/page2.xml'}
                ]
            }
        ],
    'files': []
}
