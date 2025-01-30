import logging
import requests
import sys


logging.basicConfig(
    level="DEBUG", format="%(asctime)s %(levelname)s: %(message)s", stream=sys.stdout
)

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


def create_user(user):
    requests.post(
        "http://airflow-webserver:8080/auth/fab/v1/users",
        auth=("airflow", "airflow"),
        json=user,
    )


def check_api_authorization_for_user(
    user, expected_status_code, method, endpoint, data=None, api="api/v1"
):
    api_url = f"http://airflow-webserver:8080/{api}"

    auth = (user["username"], user["password"])
    response = requests.request(method, f"{api_url}/{endpoint}", auth=auth, json=data)
    assert response.status_code == expected_status_code


def check_api_authorization(method, endpoint, data=None, api="api/v1"):
    check_api_authorization_for_user(
        user_jane_doe, 200, method=method, endpoint=endpoint, data=data, api=api
    )
    check_api_authorization_for_user(
        user_richard_roe, 403, method=method, endpoint=endpoint, data=data, api=api
    )


def check_website_authorization_for_user(user, expected_status_code):
    username = user["username"]
    password = user["password"]
    with requests.Session() as session:
        login_response = session.post(
            "http://airflow-webserver:8080/login/",
            data=f"username={username}&password={password}",
            allow_redirects=False,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        assert login_response.ok, f"Login for {username} failed"
        home_response = session.get(
            "http://airflow-webserver:8080/home", allow_redirects=False
        )
        assert (
            home_response.status_code == expected_status_code
        ), f"GET /home returned status code {home_response.status_code}, but {expected_status_code} was expected."


def test_is_authorized_configuration():
    # section == null
    check_api_authorization("GET", "config")
    # section != null
    check_api_authorization("GET", "config?section=core")


def test_is_authorized_connection():
    # conn_id == null
    check_api_authorization("GET", "connections")
    # conn_id != null
    check_api_authorization("GET", "connections/postgres_default")


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
    check_api_authorization("GET", "datasets/s3%3A%2F%2Fbucket%2Fmy-task")


def test_is_authorized_pool():
    # name == null
    check_api_authorization("GET", "pools")
    # name != null
    check_api_authorization("GET", "pools/default_pool")


def test_is_authorized_variable():
    # key != null
    check_api_authorization("POST", "variables", data={"key": "myVar", "value": "1"})
    # key == null
    check_api_authorization("GET", "variables/myVar")


def test_is_authorized_view():
    check_website_authorization_for_user(user_jane_doe, 200)
    check_website_authorization_for_user(user_richard_roe, 403)


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


# Create test users
create_user(user_jane_doe)
create_user(user_richard_roe)

test_is_authorized_configuration()
test_is_authorized_connection()
test_is_authorized_dag()
test_is_authorized_dataset()
test_is_authorized_pool()
test_is_authorized_variable()
test_is_authorized_view()
test_is_authorized_custom_view()
