import pytest
from unittest.mock import patch, MagicMock
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index


@pytest.fixture
def index_client():
    """
    Fixture to provide an index client for testing.
    This is a placeholder and should be replaced with actual client initialization.
    """
    with patch('gen3.auth.Gen3Auth.get_access_token', return_value="accesstoken:///mock_access_token"):
        yield Gen3Index(auth_provider=Gen3Auth(endpoint="https://example.com/auth"))


def test_authorization_header_present(index_client: Gen3Index):
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        # Simulate a call that would trigger requests.get
        index_client.get("1234567890abcdef1234567890abcdef")

        # Ensure requests.get was called
        assert mock_get.called
        # Get the headers from the call arguments
        auth: Gen3Auth = mock_get.call_args[1].get("auth", None)
        assert auth is not None, "Auth object should not be None"
        auth_value = auth._get_auth_value()
        assert auth_value == 'bearer accesstoken:///mock_access_token'
