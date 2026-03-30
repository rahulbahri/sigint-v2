"""
Sage Intacct connector — extracts GL entries, AR, and vendor data
via the Intacct XML API with session-based authentication.

Credentials required (Render env vars):
  SAGE_COMPANY_ID      — your Intacct company ID
  SAGE_USER_ID         — web services user login
  SAGE_USER_PASSWORD   — web services user password
  SAGE_CLIENT_ID       — your sender ID (from Sage developer agreement)
  SAGE_CLIENT_SECRET   — your sender password
"""
from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import httpx

from .base import BaseConnector, ConnectorError

_API_URL   = "https://api.intacct.com/ia/xml/xmlgw.phtml"
_COMPANY   = os.environ.get("SAGE_COMPANY_ID", "")
_USER_ID   = os.environ.get("SAGE_USER_ID", "")
_USER_PASS = os.environ.get("SAGE_USER_PASSWORD", "")
_CLIENT_ID = os.environ.get("SAGE_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("SAGE_CLIENT_SECRET", "")


def _build_request(sender_id: str, sender_pwd: str, company: str,
                   user_id: str, user_pwd: str, control_id: str, function_xml: str) -> str:
    """Build Intacct XML API request envelope."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<request>
  <control>
    <senderid>{sender_id}</senderid>
    <password>{sender_pwd}</password>
    <controlid>{control_id}</controlid>
    <uniqueid>false</uniqueid>
    <dtdversion>3.0</dtdversion>
    <includewhitespace>false</includewhitespace>
  </control>
  <operation transaction="false">
    <authentication>
      <login>
        <userid>{user_id}</userid>
        <companyid>{company}</companyid>
        <password>{user_pwd}</password>
      </login>
    </authentication>
    <content>
      <function controlid="{control_id}">{function_xml}</function>
    </content>
  </operation>
</request>"""


def _post_xml(body: str) -> ET.Element:
    """POST XML to Intacct and return root element."""
    with httpx.Client(timeout=45) as client:
        r = client.post(
            _API_URL,
            content=body.encode("utf-8"),
            headers={"Content-Type": "application/xml; encoding='UTF-8'"},
        )
    if r.status_code != 200:
        raise ConnectorError(f"Sage Intacct HTTP error: {r.status_code}")
    root = ET.fromstring(r.text)
    status = root.findtext(".//status")
    if status == "failure":
        errdesc = root.findtext(".//errormessage/error/description2") or root.findtext(".//error/description")
        raise ConnectorError(f"Sage Intacct API error: {errdesc}")
    return root


def _query_xml(object_name: str, fields: list[str], filters: str = "") -> str:
    fields_xml = "".join(f"<field>{f}</field>" for f in fields)
    filter_section = f"<filter>{filters}</filter>" if filters else ""
    return f"""
    <query>
      <object>{object_name}</object>
      <select>{fields_xml}</select>
      {filter_section}
      <pagesize>1000</pagesize>
    </query>"""


class SageIntacctConnector(BaseConnector):
    SOURCE_NAME = "sage_intacct"
    AUTH_TYPE   = "api_key"   # credential-based, no OAuth redirect

    def _creds(self, credentials: dict) -> tuple:
        return (
            credentials.get("client_id")      or _CLIENT_ID,
            credentials.get("client_secret")  or _CLIENT_SECRET,
            credentials.get("company_id")     or _COMPANY,
            credentials.get("user_id")        or _USER_ID,
            credentials.get("user_password")  or _USER_PASS,
        )

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            cid, csec, comp, uid, upwd = self._creds(credentials)
            if not all([cid, csec, comp, uid, upwd]):
                return False
            xml = _build_request(cid, csec, comp, uid, upwd, "ping",
                                 "<get_user><loginid>__</loginid></get_user>")
            _post_xml(xml)
            return True
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        cid, csec, comp, uid, upwd = self._creds(credentials)
        if not all([cid, csec, comp, uid, upwd]):
            raise ConnectorError("Sage Intacct: missing required credentials. "
                                 "Set SAGE_COMPANY_ID, SAGE_USER_ID, SAGE_USER_PASSWORD, "
                                 "SAGE_CLIENT_ID, SAGE_CLIENT_SECRET.")

        return [
            {"entity_type": "revenue",   "records": self._fetch_invoices(cid, csec, comp, uid, upwd)},
            {"entity_type": "expenses",  "records": self._fetch_apbills(cid, csec, comp, uid, upwd)},
            {"entity_type": "customers", "records": self._fetch_customers(cid, csec, comp, uid, upwd)},
        ]

    def _fetch_invoices(self, cid, csec, comp, uid, upwd) -> list[dict]:
        fn = _query_xml("ARINVOICE",
                        ["RECORDNO", "CUSTOMERID", "CUSTOMERNAME", "TOTALDUE",
                         "TOTALENTERED", "WHENDUE", "WHENCREATED", "STATE"])
        xml = _build_request(cid, csec, comp, uid, upwd, "get_invoices", fn)
        root = _post_xml(xml)
        return self._parse_list(root, "ARINVOICE")

    def _fetch_apbills(self, cid, csec, comp, uid, upwd) -> list[dict]:
        fn = _query_xml("APBILL",
                        ["RECORDNO", "VENDORID", "VENDORNAME", "TOTALDUE",
                         "TOTALENTERED", "WHENDUE", "WHENCREATED", "STATE"])
        xml = _build_request(cid, csec, comp, uid, upwd, "get_apbills", fn)
        root = _post_xml(xml)
        return self._parse_list(root, "APBILL")

    def _fetch_customers(self, cid, csec, comp, uid, upwd) -> list[dict]:
        fn = _query_xml("CUSTOMER",
                        ["CUSTOMERID", "NAME", "WHENCREATED", "STATUS", "TOTALDUE"])
        xml = _build_request(cid, csec, comp, uid, upwd, "get_customers", fn)
        root = _post_xml(xml)
        return self._parse_list(root, "CUSTOMER")

    @staticmethod
    def _parse_list(root: ET.Element, tag: str) -> list[dict]:
        records = []
        for elem in root.iter(tag):
            record = {child.tag: child.text for child in elem}
            records.append(record)
        return records
