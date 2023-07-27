from lxml import etree

ns = {
    "alto": "http://www.loc.gov/standards/alto/ns-v2#"
}


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

    def textblocks(self):
        for node in self.xmldoc.xpath("//alto:TextBlock", namespaces=ns):
            yield TextBlock(node)

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
        x = round(self.hpos * xscale)
        y = round(self.vpos * yscale)
        w = round(self.width * xscale)
        h = round(self.height * yscale)

        return x, y, w, h

    def bbox(self, scale):
        (x, y, w, h) = self.xywh(scale)
        return x, y, x + w, y + h


class TextBlock(Region):
    def lines(self):
        for node in self.element.xpath('alto:TextLine', namespaces=ns):
            yield TextLine(node)

    def text(self, scale=None):
        return "\n".join([line.text(scale) for line in self.lines()])


class TextLine(Region):
    def inlines(self):
        for node in self.element.xpath('alto:String|alto:SP|alto:HYP', namespaces=ns):
            tag = etree.QName(node.tag)
            if tag.localname == 'String':
                yield String(node)
            elif tag.localname == 'SP':
                yield Space(node)
            elif tag.localname == 'HYP':
                yield Hyphen(node)

    def text(self, scale=None):
        return ''.join([inline.text(scale) for inline in self.inlines()])


class String(Region):
    def text(self, scale=None):
        text = self.element.get('CONTENT')
        if scale is None:
            return text
        xywh = ','.join([str(i) for i in self.xywh(scale)])
        return '{0}|{1}'.format(text, xywh)


class Space(object):
    def __init__(self, element):
        self.element = element
        super(Space, self).__init__()

    def text(self, scale=None):
        return ' '


class Hyphen(object):
    def __init__(self, element):
        self.element = element
        super(Hyphen, self).__init__()

    def text(self, scale=None):
        return '\N{SOFT HYPHEN}'
