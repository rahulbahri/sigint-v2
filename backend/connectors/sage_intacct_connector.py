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

import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import httpx

from .base import BaseConnector, ConnectorError

log = logging.getLogger("connectors.sage_intacct")

_PAGE_SIZE = 1000

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


def _query_xml(object_name: str, fields: list[str], filters: str = "",
               offset: int = 0) -> str:
    """Build a <query> XML fragment with pagination support."""
    fields_xml = "".join(f"<field>{f}</field>" for f in fields)
    filter_section = f"<filter>{filters}</filter>" if filters else ""
    return f"""
    <query>
      <object>{object_name}</object>
      <select>{fields_xml}</select>
      {filter_section}
      <pagesize>{_PAGE_SIZE}</pagesize>
      <offset>{offset}</offset>
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

    def _fetch_paginated(self, cid, csec, comp, uid, upwd,
                         object_name: str, fields: list[str],
                         control_id: str, filters: str = "",
                         max_pages: int = 200) -> list[dict]:
        """Fetch all records for an object using offset-based pagination.

        After each response, checks `numremaining` on the data element.
        If > 0, queries again with an incremented offset.
        """
        all_records: list[dict] = []
        offset = 0
        for page in range(max_pages):
            fn = _query_xml(object_name, fields, filters=filters, offset=offset)
            xml = _build_request(cid, csec, comp, uid, upwd,
                                 f"{control_id}_p{page}", fn)
            root = _post_xml(xml)
            page_records = self._parse_list(root, object_name)
            all_records.extend(page_records)

            # Check if there are more records remaining
            data_elem = root.find(".//data")
            num_remaining = 0
            if data_elem is not None:
                num_remaining = int(data_elem.get("numremaining", "0"))

            log.info("[SageIntacct] %s page %d: fetched %d records "
                     "(total %d, remaining %d)",
                     object_name, page + 1, len(page_records),
                     len(all_records), num_remaining)

            if num_remaining <= 0 or len(page_records) < _PAGE_SIZE:
                break
            offset += _PAGE_SIZE
        else:
            log.warning("[SageIntacct] %s hit max page limit (%d), "
                        "some records may be missing",
                        object_name, max_pages)

        return all_records

    def _fetch_invoices(self, cid, csec, comp, uid, upwd) -> list[dict]:
        return self._fetch_paginated(
            cid, csec, comp, uid, upwd,
            object_name="ARINVOICE",
            fields=["RECORDNO", "CUSTOMERID", "CUSTOMERNAME", "TOTALDUE",
                     "TOTALENTERED", "WHENDUE", "WHENCREATED", "STATE"],
            control_id="get_invoices",
        )

    def _fetch_apbills(self, cid, csec, comp, uid, upwd) -> list[dict]:
        return self._fetch_paginated(
            cid, csec, comp, uid, upwd,
            object_name="APBILL",
            fields=["RECORDNO", "VENDORID", "VENDORNAME", "TOTALDUE",
                     "TOTALENTERED", "WHENDUE", "WHENCREATED", "STATE"],
            control_id="get_apbills",
        )

    def _fetch_customers(self, cid, csec, comp, uid, upwd) -> list[dict]:
        return self._fetch_paginated(
            cid, csec, comp, uid, upwd,
            object_name="CUSTOMER",
            fields=["CUSTOMERID", "NAME", "WHENCREATED", "STATUS", "TOTALDUE"],
            control_id="get_customers",
        )

    @staticmethod
    def _parse_list(root: ET.Element, tag: str) -> list[dict]:
        records = []
        for elem in root.iter(tag):
            record = {child.tag: child.text for child in elem}
            records.append(record)
        return records
