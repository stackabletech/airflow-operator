# $NAMESPACE will be replaced with the namespace of the test case.

import logging
import requests
import sys
from bs4 import BeautifulSoup

logging.basicConfig(
    level="DEBUG", format="%(asctime)s %(levelname)s: %(message)s", stream=sys.stdout
)

session = requests.Session()

# Click on "Sign In with keycloak" in Airflow
login_page = session.get("http://airflow-webserver:8080/login/keycloak?next=")

assert login_page.ok, "Redirection from Airflow to Keycloak failed"
assert login_page.url.startswith(
    "https://keycloak.$NAMESPACE.svc.cluster.local:8443/realms/test/protocol/openid-connect/auth?response_type=code&client_id=airflow"
), "Redirection to the Keycloak login page expected"

# Enter username and password into the Keycloak login page and click on "Sign In"
login_page_html = BeautifulSoup(login_page.text, "html.parser")
authenticate_url = login_page_html.form["action"]
welcome_page = session.post(
    authenticate_url, data={"username": "jane.doe", "password": "T8mn72D9"}
)

assert welcome_page.ok, "Login failed"
assert (
    welcome_page.url == "http://airflow-webserver:8080/home"
), "Redirection to the Airflow home page expected"

# Open the user information page in Airflow
userinfo_page = session.get("http://airflow-webserver:8080/users/userinfo/")

assert userinfo_page.ok, "Retrieving user information failed"
assert (
    userinfo_page.url == "http://airflow-webserver:8080/users/userinfo/"
), "Redirection to the Airflow user info page expected"

# Expect the user data provided by Keycloak in Airflow
userinfo_page_html = BeautifulSoup(userinfo_page.text, "html.parser")
table_rows = userinfo_page_html.find_all("tr")
user_data = {tr.find("th").text: tr.find("td").text for tr in table_rows}

assert (
    user_data["First Name"] == "Jane"
), "The first name of the user in Airflow should match the one provided by Keycloak"
assert (
    user_data["Last Name"] == "Doe"
), "The last name of the user in Airflow should match the one provided by Keycloak"
assert (
    user_data["Email"] == "jane.doe@stackable.tech"
), "The email of the user in Airflow should match the one provided by Keycloak"
