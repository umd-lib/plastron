from plastron.utils import envsubst


def test_simple_strings():
    env = {'FOO': 'a', 'BAR': 'z'}
    assert envsubst('${FOO}bc', env) == 'abc'
    assert envsubst('${FOO}bc${BAR}yx', env) == 'abczyx'


def test_unknown_variable_name():
    assert envsubst('${FOO}qrs', {}) == '${FOO}qrs'


def test_lists():
    env = {'FOO': 'a', 'BAR': 'z'}
    assert envsubst(['${FOO}', '${BAR}'], env) == ['a', 'z']
    assert envsubst(['${FOO}', '${BAR}', '${BAZ}'], env) == ['a', 'z', '${BAZ}']


def test_dicts():
    env = {'FOO': 'a', 'BAR': 'z'}
    assert envsubst({'foo': '${FOO}', 'bar': '${BAR}'}, env) == {'foo': 'a', 'bar': 'z'}
    assert envsubst({'foo': '${FOO}', 'bar': '${BAZ}'}, env) == {'foo': 'a', 'bar': '${BAZ}'}


def test_deep_structure():
    env = {'FOO': 'a'}
    assert envsubst([{'foo': ['${FOO}']}], env) == [{'foo': ['a']}]
