import logging
import requests
import sys
import os

logging.basicConfig(
    level="DEBUG", format="%(asctime)s %(levelname)s: %(message)s", stream=sys.stdout
)

log = logging.getLogger(__name__)

# user to headers mapping
headers: dict[str, dict[str, str]] = {}

# Jane Doe has access to specific resources.
user_jane_doe = {
    "first_name": "Jane",
    "last_name": "Doe",
    "username": "jane.doe",
    "email": "jane.doe@stackable.tech",
    "roles": [{"name": "User"}],
    "password": "T8mn72D9",
}
# Richard Roe has no access.
user_richard_roe = {
    "first_name": "Richard",
    "last_name": "Roe",
    "username": "richard.roe",
    "email": "richard.roe@stackable.tech",
    "roles": [{"name": "User"}],
    "password": "NvfpU518",
}

url = "http://airflow-webserver-default:8080"

def obtain_access_token(user: dict[str, str]) -> str:
    token_url = f"{url}/auth/token"

    data = {"username": user["username"], "password": user["password"]}

    headers = {"Content-Type": "application/json"}

    response = requests.post(token_url, headers=headers, json=data)

    if response.status_code == 200 or response.status_code == 201:
        token_data = response.json()
        access_token = token_data["access_token"]
        log.info(f"Got access token: {access_token}")
        return access_token
    else:
        log.error(f"Failed to obtain access token: {response.status_code} - {response.text}")
        sys.exit(1)

def assert_status_code(msg, left, right):
    if left != right:
        raise AssertionError(f"{msg}\n\tleft: {left}\n\tright: {right}")

def check_api_authorization_for_user(
    user, expected_status_code, method, endpoint, data=None, api="api/v2"
):
    api_url = f"{url}/{api}"

    response = requests.request(method, f"{api_url}/{endpoint}", headers=headers[user["email"]], json=data)

    assert_status_code(f"Unexpected status code for {user["email"]=}", response.status_code, expected_status_code)


def check_api_authorization(method, endpoint, expected_status_code=200, data=None, api="api/v2"):
    check_api_authorization_for_user(
        user_jane_doe, expected_status_code, method=method, endpoint=endpoint, data=data, api=api
    )
    check_api_authorization_for_user(
        user_richard_roe, 403, method=method, endpoint=endpoint, data=data, api=api
    )


def check_website_authorization_for_user(user, expected_status_code):
    username = user["username"]
    password = user["password"]
    with requests.Session() as session:
        login_response = session.post(
            url_login,
            data=f"username={username}&password={password}",
            allow_redirects=True,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        assert login_response.ok, f"Login for {username} failed"
        home_response = session.get(f"{url}/home", allow_redirects=True)
        assert_status_code(
            f"GET /home for user [{username}] failed",  home_response.status_code, expected_status_code
        )

def test_is_authorized_configuration():
    # section == null
    check_api_authorization("GET", "config")
    # section != null
    check_api_authorization("GET", "config?section=core")


def test_is_authorized_connection():
    # conn_id == null
    check_api_authorization("GET", "connections")


def test_is_authorized_dag():
    # access_entity == null and id == null
    # There is no API endpoint to test this case.

    # access_entity == null and id != null
    check_api_authorization("GET", "dags/example_trigger_target_dag")

    # access_entity != null and id == null
    # Check "GET /dags/~/dagRuns" because access to "GET /dags" is always allowed
    check_api_authorization("GET", "dags/~/dagRuns")

    # access_entity != null and id != null
    check_api_authorization("GET", "dags/example_trigger_target_dag/dagRuns")


def test_is_authorized_dataset():
    # uri == null
    check_api_authorization("GET", "datasets")
    # uri != null
    check_api_authorization("GET", "datasets/s3%3A%2F%2Fdag1%2Foutput_1.txt")


def test_is_authorized_pool():
    # name == null
    check_api_authorization("GET", "pools")
    # name != null
    check_api_authorization("GET", "pools/default_pool")


def test_is_authorized_variable():
    # key != null
    check_api_authorization("POST", "variables", 201, data={"key": "myVar", "value": "1"})
    # key == null
    check_api_authorization("GET", "variables/myVar")


def test_is_authorized_view():
    check_website_authorization_for_user(user_jane_doe, 200)
    check_website_authorization_for_user(user_richard_roe, 200)


def test_is_authorized_custom_view():
    user_jane_doe_patched = user_jane_doe.copy()
    user_jane_doe_patched["email"] = "jane@stackable.tech"
    check_api_authorization_for_user(
        user_jane_doe,
        200,
        "PATCH",
        "users/jane.doe?update_mask=email",
        data=user_jane_doe_patched,
        api="/auth/fab/v1",
    )

    user_richard_roe_patched = user_richard_roe.copy()
    user_richard_roe_patched["email"] = "richard@stackable.tech"
    check_api_authorization_for_user(
        user_richard_roe,
        403,
        "PATCH",
        "users/richard.roe?update_mask=email",
        data=user_richard_roe_patched,
        api="/auth/fab/v1",
    )


access_token_jane_doe = obtain_access_token(user_jane_doe)
headers[user_jane_doe["email"]] = {
    "Authorization": f"Bearer {access_token_jane_doe}",
    "Content-Type": "application/json",
}
access_token_richard_roe = obtain_access_token(user_richard_roe)
headers[user_richard_roe["email"]] = {
    "Authorization": f"Bearer {access_token_richard_roe}",
    "Content-Type": "application/json",
}

airflow_version = os.environ["AIRFLOW_VERSION"]

if airflow_version.startswith("3"):

    url_login = f"{url}/auth/login"
    
    test_is_authorized_configuration()
    test_is_authorized_connection()
    test_is_authorized_dag()
    test_is_authorized_pool()
    test_is_authorized_variable()
    test_is_authorized_view()
    # test_is_authorized_custom_view() # patching users with the FAB API is not supported in airflow 3
    # test_is_authorized_dataset() # no datasets on airflow 3
else:

    url_login=f"{url}/login/"
    
    test_is_authorized_configuration()
    test_is_authorized_connection()
    test_is_authorized_dag()
    test_is_authorized_dataset()
    test_is_authorized_pool()
    test_is_authorized_variable()
    test_is_authorized_view()
    test_is_authorized_custom_view()
