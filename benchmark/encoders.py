import pytest


@pytest.fixture(
    params=[
        "string",
        b"bytess",
        10**5,
        10 / 3,
    ]
)
def data(request):
    return request.param


