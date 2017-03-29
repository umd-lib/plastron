from lxml import etree

ns = { "alto": "http://www.loc.gov/standards/alto/ns-v2#" }

class ALTOResource(object):
    def __init__(self, xmldoc, image_resolution):
        self.xmldoc = xmldoc
        unit = xmldoc.xpath('/alto:alto/alto:Description/alto:MeasurementUnit', namespaces=ns)[0].text
        xres = image_resolution[0]
        yres = image_resolution[1]

        if unit == 'inch1200':
            self.scale = (xres / 1200.0, yres / 1200.0)
        elif unit == 'mm10':
            self.scale = (xres / 254.0, yres / 254.0)
        elif unit == 'pixel':
            self.scale = (1, 1)
        else:
            raise Exception("Unknown MeasurementUnit " + unit)

    def textblock(self, id):
        return TextBlock(self.xmldoc.xpath("//alto:TextBlock[@ID=$id]", id=id, namespaces=ns)[0])

class Region(object):
    def __init__(self, element):
        self.element = element
        self.id = self.element.get('ID')
        self.hpos = int(self.element.get('HPOS'))
        self.vpos = int(self.element.get('VPOS'))
        self.width = int(self.element.get('WIDTH'))
        self.height = int(self.element.get('HEIGHT'))

    def xywh(self, scale):
        xscale = scale[0]
        yscale = scale[1]
        x = round(self.hpos   * xscale)
        y = round(self.vpos   * yscale)
        w = round(self.width  * xscale)
        h = round(self.height * yscale)

        return (x, y, w, h)

    def bbox(self, scale):
        (x, y, w, h) = self.xywh(scale)
        return (x, y, x+w, y+h)

class TextBlock(Region):
    def lines(self):
        for node in self.element.xpath('alto:TextLine', namespaces=ns):
            yield TextLine(node)

    def text(self):
        return "\n".join([ line.text() for line in self.lines() ])

class TextLine(Region):
    def text(self):
        text = ''
        for node in self.element.xpath('alto:String|alto:SP|alto:HYP', namespaces=ns):
            tag = etree.QName(node.tag)
            if tag.localname == 'SP':
                text += ' '
            else:
                text += node.get('CONTENT')
        return text
