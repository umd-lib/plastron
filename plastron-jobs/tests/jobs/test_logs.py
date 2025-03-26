import pytest

from plastron.jobs import ItemLog
from plastron.jobs.logs import AppendableSequence, NullLog, ItemLogError


@pytest.fixture
def item_log(datadir):
    return ItemLog(
        filename=(datadir / 'item_log.csv'),
        fieldnames=['id', 'title'],
        keyfield='id',
    )


def test_appendable_sequence_is_abstract():
    with pytest.raises(TypeError):
        AppendableSequence()


def test_null_log():
    log = NullLog()
    assert len(log) == 0
    log.append({'foo': 'bar'})
    assert len(log) == 0
    assert {'foo': 'bar'} not in log
    with pytest.raises(IndexError):
        _ = log[0]


def test_item_log_new_file(datadir):
    log = ItemLog(
        filename=(datadir / 'new_log.csv'),
        fieldnames=['id', 'title'],
        keyfield='id',
    )
    assert not log.exists()
    log.create()
    assert log.exists()


def test_existing_item_log(item_log):
    assert item_log.exists()
    assert 'foo' in item_log
    assert len(item_log) == 1
    assert next(iter(item_log)) == {'id': 'foo', 'title': 'The Adventures of Foo'}
    assert item_log[0] == {'id': 'foo', 'title': 'The Adventures of Foo'}


def test_item_log_index_error(item_log):
    with pytest.raises(IndexError):
        _ = item_log[2]


def test_item_log_append(item_log):
    assert len(item_log) == 1
    item_log.append({'id': 'bar', 'title': 'The Bar Strikes Back'})
    assert len(item_log) == 2


def test_item_log_writerow(item_log):
    assert len(item_log) == 1
    item_log.writerow({'id': 'bar', 'title': 'The Bar Strikes Back'})
    assert len(item_log) == 2


def test_item_log_mismatched_fieldnames(datadir, caplog):
    log = ItemLog(
        filename=(datadir / 'item_log.csv'),
        fieldnames=['not', 'real'],
        keyfield='id',
    )
    iter(log)
    assert 'do not match expected fieldnames' in caplog.text


def test_item_log_bad_keyfield(datadir):
    with pytest.raises(ItemLogError):
        ItemLog(
            filename=(datadir / 'item_log.csv'),
            fieldnames=['id', 'title'],
            keyfield='nope',
        )
