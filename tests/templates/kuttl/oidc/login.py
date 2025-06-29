# $NAMESPACE will be replaced with the namespace of the test case.

import logging
import requests
import sys
import os
from bs4 import BeautifulSoup

logging.basicConfig(
    level="DEBUG", format="%(asctime)s %(levelname)s: %(message)s", stream=sys.stdout
)

log = logging.getLogger(__name__)


def assert_equal(a, b, msg):
    if a != b:
        raise AssertionError(f"{msg}\n\tleft: {a}\n\tright: {b}")


def assert_startwith(a, b, msg):
    if not a.startswith(b):
        raise AssertionError(f"{msg}\n\tleft: {a}\n\tright: {b}")


def login_page(base_url: str, airflow_version: str) -> str:
    if airflow_version.startswith("3"):
        return f"{base_url}/auth/login/keycloak?next="
    else:
        return f"{base_url}/login/keycloak?next="


def userinfo_page(base_url: str, airflow_version: str) -> str:
    if airflow_version.startswith("3"):
        return f"{base_url}/auth/users/userinfo/"
    else:
        return f"{base_url}/users/userinfo/"


session = requests.Session()
url = "http://airflow-webserver:8080"

# Click on "Sign In with keycloak" in Airflow
login_page = session.get(login_page(url, os.environ["AIRFLOW_VERSION"]))

assert login_page.ok, "Redirection from Airflow to Keycloak failed"

assert_startwith(
    login_page.url,
    f"https://keycloak1.{os.environ['NAMESPACE']}.svc.cluster.local:8443/realms/test1/protocol/openid-connect/auth?response_type=code&client_id=airflow1",
    "Redirection to the Keycloak login page expected",
)

# Enter username and password into the Keycloak login page and click on "Sign In"
login_page_html = BeautifulSoup(login_page.text, "html.parser")
authenticate_url = login_page_html.form["action"]
welcome_page = session.post(
    authenticate_url, data={"username": "jane.doe", "password": "T8mn72D9"}
)

assert welcome_page.ok, "Login failed"
assert_equal(
    welcome_page.url, f"{url}/", "Redirection to the Airflow home page expected"
)

# Open the user information page in Airflow
userinfo_url = userinfo_page(url, os.environ["AIRFLOW_VERSION"])
userinfo_page = session.get(userinfo_url)

assert userinfo_page.ok, "Retrieving user information failed"
assert_equal(
    userinfo_page.url,
    userinfo_url,
    "Redirection to the Airflow user info page expected",
)

# Expect the user data provided by Keycloak in Airflow
userinfo_page_html = BeautifulSoup(userinfo_page.text, "html.parser")
table_rows = userinfo_page_html.find_all("tr")
user_data = {tr.find("th").text: tr.find("td").text for tr in table_rows}

log.debug(f"{user_data=}")

assert user_data["First Name"] == "Jane", (
    "The first name of the user in Airflow should match the one provided by Keycloak"
)
assert user_data["Last Name"] == "Doe", (
    "The last name of the user in Airflow should match the one provided by Keycloak"
)
assert user_data["Email"] == "jane.doe@stackable.tech", (
    "The email of the user in Airflow should match the one provided by Keycloak"
)

log.info("OIDC login test passed")

# Later this can be extended to use different OIDC providers (currently only Keycloak is
# supported)
#
# It would be beneficial if the second OAuth provider keycloak2 could
# also be tested. This would ensure that the Airflow configuration is
# correct. The problem is that the Flask-AppBuilder (and hence Airflow)
# do not support multiple OAuth providers with the same name. But
# keycloak1 and keycloak2 use the same name, namely "keycloak":
#
#  OAUTH_PROVIDERS = [
#    { 'name': 'keycloak',
#      'icon': 'fa-key',
#      'token_key': 'access_token',
#      'remote_app': {
#        'client_id': os.environ.get('OIDC_728D9B504A6E9A10_CLIENT_ID'),
#        'client_secret': os.environ.get('OIDC_728D9B504A6E9A10_CLIENT_SECRET'),
#        'client_kwargs': {
#          'scope': 'email openid profile'
#        },
#        'api_base_url': 'https://keycloak1.kuttl.svc.cluster.local:8443/realms/test1/protocol/',
#        'server_metadata_url': 'https://keycloak1.kuttl.svc.cluster.local:8443/realms/test1/.well-known/openid-configuration',
#      },
#    },
#    { 'name': 'keycloak',
#      'icon': 'fa-key',
#      'token_key': 'access_token',
#      'remote_app': {
#        'client_id': os.environ.get('OIDC_607BA683B09BC0B8_CLIENT_ID'),
#        'client_secret': os.environ.get('OIDC_607BA683B09BC0B8_CLIENT_SECRET'),
#        'client_kwargs': {
#          'scope': 'email openid profile'
#        },
#        'api_base_url': 'https://keycloak2.kuttl.svc.cluster.local:8443/realms/test2/protocol/',
#        'server_metadata_url': 'https://keycloak2.kuttl.svc.cluster.local:8443/realms/test2/.well-known/openid-configuration',
#      },
#    }
#    ]
#
# This name is set in the operator and cannot be changed. The reason is
# that the name is also used in Flask-AppBuilder to determine how the
# user information must be interpreted.
#
# Airflow actually shows two "Sign In with keycloak" buttons in this
# test but the second one cannot be clicked.
#
# It is nevertheless useful to have two Keycloak instances in this test
# because it ensures that several authentication entries can be
# specified, no volumes or volume mounts are added twice, and that the
# configuration is correct to the extent that Airflow does not complain
# about it.
