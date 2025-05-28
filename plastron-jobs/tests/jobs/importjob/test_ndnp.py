import pytest

from plastron.jobs.importjob.ndnp import NDNPBatch, get_issue_data, write_import_csv
from plastron.repo import DataReadError


def test_batch(datadir):
    batch = NDNPBatch(datadir, 'small.xml')
    assert batch.root_dir == datadir
    issues = list(batch.issues())
    assert len(issues) == 5


def test_issue(datadir):
    batch = NDNPBatch(datadir, 'small.xml')
    issue = next(batch.issues())
    assert issue.lccn == 'sn90057049'
    assert issue.issue_date == '1926-01-12'
    assert issue.edition_order == 1
    assert issue.mets_path == datadir / 'sn90057049/7637/1926011201/1926011201_1.xml'
    assert issue.article_mets_path == datadir / 'Article-Level/sn90057049/7637/1926011201/1926011201.xml'
    assert issue.get_volume() == '6'
    assert issue.get_issue() == '13'
    assert issue.get_edition() == '1'
    assert issue.get_title() == 'The diamondback (College Park, Md.), 1926-01-12'


def test_non_existent_issue_detail(datadir):
    batch = NDNPBatch(datadir, 'small.xml')
    issue = next(batch.issues())
    assert issue._get_detail_number('FAKE_DETAIL') is None


def test_get_issue_data(datadir):
    batch = NDNPBatch(datadir, 'small.xml')
    issue = next(batch.issues())
    data = get_issue_data(issue)
    assert data == {
        'Title': 'The diamondback (College Park, Md.), 1926-01-12',
        'Date': '1926-01-12',
        'Volume': '6',
        'Issue': '13',
        'Edition': '1',
        'Rights Statement': 'http://vocab.lib.umd.edu/rightsStatement#InC-NC',
        'FILES': ';'.join([
            '<preservation>sn90057049/7637/1926011201/0002.tif',
            '<service>sn90057049/7637/1926011201/0002.jp2',
            '<derivative>sn90057049/7637/1926011201/0002.pdf',
            '<ocr>sn90057049/7637/1926011201/0002.xml',
            '<preservation>sn90057049/7637/1926011201/0003.tif',
            '<service>sn90057049/7637/1926011201/0003.jp2',
            '<derivative>sn90057049/7637/1926011201/0003.pdf',
            '<ocr>sn90057049/7637/1926011201/0003.xml',
            '<preservation>sn90057049/7637/1926011201/0004.tif',
            '<service>sn90057049/7637/1926011201/0004.jp2',
            '<derivative>sn90057049/7637/1926011201/0004.pdf',
            '<ocr>sn90057049/7637/1926011201/0004.xml',
            '<preservation>sn90057049/7637/1926011201/0005.tif',
            '<service>sn90057049/7637/1926011201/0005.jp2',
            '<derivative>sn90057049/7637/1926011201/0005.pdf',
            '<ocr>sn90057049/7637/1926011201/0005.xml',
        ]),
        'ITEM_FILES': ';'.join([
            '<metadata>sn90057049/7637/1926011201/1926011201_1.xml',
            '<metadata>Article-Level/sn90057049/7637/1926011201/1926011201.xml',
        ]),
    }


def test_write_import_csv(datadir, capsys):
    batch = NDNPBatch(datadir, 'small.xml')
    write_import_csv(batch)
    captured = capsys.readouterr()
    assert 'Title,Date,Volume,Issue,Edition,Rights Statement,FILES,ITEM_FILES\r\n' in captured.out
    assert len(captured.out.split('\r\n')) == 7


def test_batch_dir_not_found():
    with pytest.raises(DataReadError):
        NDNPBatch('FAKE', 'DOES_NOT_EXIST.xml')


def test_batch_dir_not_a_dir(datadir):
    with pytest.raises(DataReadError):
        NDNPBatch(datadir / 'plain.txt', 'DOES_NOT_EXIST.xml')


def test_batch_file_not_found(datadir):
    with pytest.raises(DataReadError):
        NDNPBatch(datadir, 'DOES_NOT_EXIST.xml')


def test_batch_file_not_xml(datadir):
    with pytest.raises(DataReadError):
        NDNPBatch(datadir, 'plain.txt')
