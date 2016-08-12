#!/usr/bin/env python3

my_item = {
    'title': 'Diamondback',
    'volume': 1,
    'issue': 1,
    'edition': 1,
    'pages': [
        {
        'metadata': (1, 'path/to/page/1/'),
        'files': {
            'master': 'path/to/page/1/page1.jp2',
            'derivative': 'path/to/page/1/page1.jpg',
            'embedded': 'path/to/page/1/page1.pdf',
            'ocr': 'path/to/page/1/page1.xml'
            }
        },{
        'metadata': (2, 'path/to/page/2/'),
        'files': {
            'master': 'path/to/page/2/page2.jp2',
            'derivative': 'path/to/page/2/page2.jpg',
            'embedded': 'path/to/page/2/page2.pdf',
            'ocr': 'path/to/page/2/page2.xml'
            }
        }
    ]
}
