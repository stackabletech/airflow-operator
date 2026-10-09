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


def auth_cookie(session: requests.Session, airflow_version: str):
    # Airflow 3's api-server authenticates via this JWT cookie; there is no equivalent on
    # Airflow 2, which uses Flask's own session cookie instead.
    if not airflow_version.startswith("3"):
        return None
    return next((c for c in session.cookies if c.name == "_token"), None)


def login(url: str, airflow_version: str) -> requests.Session:
    """Log in to Airflow via Keycloak at the given base URL and check that the OIDC data
    Keycloak provides ends up correctly reflected in Airflow. Returns the session so callers
    can inspect e.g. cookies afterwards."""

    session = requests.Session()

    # Click on "Sign In with keycloak" in Airflow
    login_response = session.get(login_page(url, airflow_version))

    assert login_response.ok, "Redirection from Airflow to Keycloak failed"

    assert_startwith(
        login_response.url,
        f"https://keycloak1.{os.environ['NAMESPACE']}.svc.cluster.local:8443/realms/test1/protocol/openid-connect/auth?response_type=code&client_id=airflow1",
        "Redirection to the Keycloak login page expected",
    )

    # Enter username and password into the Keycloak login page and click on "Sign In"
    login_response_html = BeautifulSoup(login_response.text, "html.parser")
    authenticate_url = login_response_html.form["action"]
    welcome_response = session.post(
        authenticate_url, data={"username": "jane.doe", "password": "T8mn72D9"}
    )

    assert welcome_response.ok, "Login failed"
    assert_equal(
        welcome_response.url, f"{url}/", "Redirection to the Airflow home page expected"
    )

    # Open the user information page in Airflow
    userinfo_url = userinfo_page(url, airflow_version)
    userinfo_response = session.get(userinfo_url)

    assert userinfo_response.ok, "Retrieving user information failed"
    assert_equal(
        userinfo_response.url,
        userinfo_url,
        "Redirection to the Airflow user info page expected",
    )

    # Expect the user data provided by Keycloak in Airflow
    userinfo_response_html = BeautifulSoup(userinfo_response.text, "html.parser")
    table_rows = userinfo_response_html.find_all("tr")
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

    return session


airflow_version = os.environ["AIRFLOW_VERSION"]

# Log in directly against the webserver, bypassing the reverse proxy.
direct_session = login("http://airflow-webserver:8080", airflow_version)
log.info("Direct OIDC login test passed")

# The webserver is plain HTTP here, so its auth cookie must not be marked Secure.
direct_cookie = auth_cookie(direct_session, airflow_version)
if airflow_version.startswith("3"):
    assert direct_cookie is not None, "Expected an auth cookie after a successful login"
    assert not direct_cookie.secure, (
        "The auth cookie must not be marked Secure when accessed directly over HTTP"
    )

# Log in again through the TLS-terminating reverse proxy installed in
# 45-install-reverse-proxy.yaml, which is covered by trustedProxies in install-airflow.yaml.j2.
# If the webserver did not trust the proxy's forwarded headers, this either fails outright
# (the OIDC redirect_uri would be built with the wrong scheme) or silently loses the point of
# running behind a proxy (the auth cookie would not be marked Secure despite being sent over
# TLS). See docs/modules/airflow/pages/usage-guide/reverse-proxy.adoc.
proxy_url = (
    f"https://airflow-reverse-proxy.{os.environ['NAMESPACE']}.svc.cluster.local:8443"
)
proxied_session = login(proxy_url, airflow_version)
log.info("Reverse-proxied OIDC login test passed")

proxied_cookie = auth_cookie(proxied_session, airflow_version)
if airflow_version.startswith("3"):
    assert proxied_cookie is not None, (
        "Expected an auth cookie after a successful login"
    )
    assert proxied_cookie.secure, (
        "The auth cookie must be marked Secure when trustedProxies allows the webserver to "
        "trust the reverse proxy's X-Forwarded-Proto: https"
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
