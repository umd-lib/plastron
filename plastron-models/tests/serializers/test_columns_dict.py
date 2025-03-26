from plastron.serializers.csv import ColumnsDict


def test_columns_dict():
    columns = ColumnsDict()
    columns.update({'foo': 'bar'})
    columns.index.append('log message')
    assert 'foo' in columns
    assert columns['foo'] == 'bar'
    assert 'log message' in columns.index
