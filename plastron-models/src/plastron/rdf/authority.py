import plastron.validation.rules
from plastron.namespaces import edm, geo, rdfs, owl
from plastron.rdf import ldp, rdf


def create_authority(graph, subject):
    if (subject, rdf.ns.type, edm.Place) in graph:
        return Place.from_graph(graph, subject)
    else:
        return LabeledThing.from_graph(graph, subject)


@rdf.data_property('label', rdfs.label)
@rdf.object_property('same_as', owl.sameAs)
class LabeledThing(ldp.Resource):
    VALIDATION_RULESET = {
        'label': {
            'required': True
        },
        'same_as': {}
    }

    def validate(self, parent_prop, result):
        '''
        Validates this object based on its internal ruleset, populating
        the provided result object with the result of the validation.
        '''
        ruleset = self.VALIDATION_RULESET
        for field, rules in ruleset.items():
            for rule_name, arg in rules.items():
                rule = getattr(plastron.validation.rules, rule_name)
                prop = getattr(self, field)
                if rule(prop, arg):
                    result.passes(parent_prop, rule, arg)
                else:
                    result.fails(parent_prop, rule, arg)


@rdf.data_property('lat', geo.lat)
@rdf.data_property('lon', geo.long)
class Place(LabeledThing):
    pass
