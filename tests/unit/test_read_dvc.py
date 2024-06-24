from gen3_tracker.git import to_dvc, DVCItem


def test_read_dvc():
    dvc = to_dvc('tests/fixtures/hello.txt.dvc')
    assert dvc
    assert dvc.outs
    assert dvc.outs[0].path == 'my-project-data/hello.txt'


def test_read_dvc_item():
    _ = {'hash': 'md5', 'is_symlink': False, 'md5': 'b1946ac92492d2347c6235b4d2611184', 'mime': 'text/plain', 'modified': '2024-04-30T17:46:30.819143+00:00', 'path': 'my-project-data/hello.txt', 'realpath': '/Users/walsbr/aced/g3t-git/attic/cbds-test39/my-project-data/hello.txt', 'size': 6}
    item = DVCItem(**_)
    assert item
    assert item.hash == 'md5'
