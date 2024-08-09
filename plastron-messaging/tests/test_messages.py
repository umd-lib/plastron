from plastron.messaging.messages import PlastronCommandMessage, PlastronErrorMessage, PlastronMessage, \
    PlastronResponseMessage


def test_plastron_message_no_body():
    msg = PlastronMessage(message_id='foo', job_id='1')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.persistent == 'true'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['persistent'] == 'true'


def test_plastron_message_string_body():
    msg = PlastronMessage(message_id='foo', job_id='1', body='hello world')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.persistent == 'true'
    assert msg.body == 'hello world'
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['persistent'] == 'true'


def test_plastron_message_dict_body():
    msg = PlastronMessage(message_id='foo', job_id='1', body={'greeting': 'hello world'})
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.persistent == 'true'
    assert msg.body == '{"greeting": "hello world"}'
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['persistent'] == 'true'


def test_plastron_message_not_persistent():
    msg = PlastronMessage(message_id='foo', job_id='1', persistent='false')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.persistent == 'false'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['persistent'] == 'false'


def test_plastron_response_message():
    msg = PlastronResponseMessage(state='Done', message_id='foo', job_id='1')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.state == 'Done'
    assert msg.persistent == 'true'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['PlastronJobState'] == 'Done'
    assert msg.headers['persistent'] == 'true'


def test_plastron_error_message():
    msg = PlastronErrorMessage(error='File Not Found', message_id='foo', job_id='1')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.error == 'File Not Found'
    assert msg.persistent == 'true'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['PlastronJobError'] == 'File Not Found'
    assert msg.headers['persistent'] == 'true'


def test_plastron_command_message():
    msg = PlastronCommandMessage(command='run', message_id='foo', job_id='1')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.command == 'run'
    assert msg.persistent == 'true'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['PlastronCommand'] == 'run'
    assert msg.headers['persistent'] == 'true'


def test_plastron_command_message_with_args():
    msg = PlastronCommandMessage(command='run', args={'size': 38, 'color': 'blue'}, message_id='foo', job_id='1')
    assert msg.id == 'foo'
    assert msg.job_id == '1'
    assert msg.command == 'run'
    assert msg.args == {'size': 38, 'color': 'blue'}
    assert msg.persistent == 'true'
    assert msg.body == ''
    assert msg.headers['message-id'] == 'foo'
    assert msg.headers['PlastronJobId'] == '1'
    assert msg.headers['PlastronCommand'] == 'run'
    assert msg.headers['PlastronArg-size'] == 38
    assert msg.headers['PlastronArg-color'] == 'blue'
    assert msg.headers['persistent'] == 'true'
