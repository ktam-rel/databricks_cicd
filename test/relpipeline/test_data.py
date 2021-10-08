from relpipeline.data import compute


def test_compute():
    expected = 124
    actual = compute(1)
    assert expected == actual

def test_compute_without_waiting(mocker):
    api_return_value = 45231
    mocker.patch(
        'relpipeline.data.expensive_api_call',
        return_value = api_return_value
    )
    
    expected = 45232
    actual = compute(1)
    assert expected == actual