# coding: utf-8
# Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
# This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl 
# or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

import requests
from oci.config import from_file
from oci.signer import AbstractBaseSigner, load_private_key, load_private_key_from_file
import base64

import logging
from http.client import HTTPConnection  # py3

## This is a custom class based on the original Signer() class from signer.py file to allow calling from API Gateway on the signing process.
class MySigner(AbstractBaseSigner):
    def __init__(self, tenancy, user, fingerprint, private_key_file_location, pass_phrase=None, private_key_content=None):
        self.api_key = tenancy + "/" + user + "/" + fingerprint

        if private_key_content:
            self.private_key = load_private_key(private_key_content, pass_phrase)
        else:
            self.private_key = load_private_key_from_file(private_key_file_location, pass_phrase)

        generic_headers = ["date", "(request-target)"]
        body_headers = ["content-length", "content-type", "x-content-sha256"]
        self.create_signers(self.api_key, self.private_key, generic_headers, body_headers)

def createGroupCursor(endpoint):
   body = {
     "groupName": "MyTestGroupName",
     "type": "TRIM_HORIZON"
     }
   
   response = requests.post(endpoint+'/groupCursors', json=body, auth=auth)
   response.raise_for_status()
   return response.json()['value']

def getMessages(endpoint, cursor_id):
   
   headers = {'Content-type': 'application/json'}

   params = {
      "cursor": cursor_id,
      "limit": 1000
    } 
   response = requests.get(endpoint+'/messages',  auth=auth, params=params, headers=headers)
   response.raise_for_status()
   return response.json(), response.headers["opc-next-cursor"]

log = logging.getLogger('urllib3')
log.setLevel(logging.DEBUG)

# logging from urllib3 to console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

# print statements from `http.client.HTTPConnection` to console/stdout
HTTPConnection.debuglevel = 1

# Load config from your local OCI CLI instalation, usually located at ~/.oci/config 
config = from_file()

# Any request to OCI API must be signed, refer https://docs.oracle.com/en-us/iaas/Content/API/Concepts/signingrequests.htm#Request_Signatures
auth = MySigner(
tenancy=config['tenancy'],
user=config['user'],
fingerprint=config['fingerprint'],
private_key_file_location=config['key_file'],
pass_phrase=config['pass_phrase']
)

# set your endpoint and stream OCID properly
base_endpoint = 'https://bn******uu.apigateway.us-ashburn-1.oci.customer-oci.com/20180418/streams/'
stream_ID = 'ocid1.stream.oc1.iad.****aaaa'

endpoint = base_endpoint+stream_ID
cursor = createGroupCursor(endpoint)
messageValue,next_cursor = getMessages(endpoint, cursor)

for message in messageValue:
    decoded_bytes = base64.b64decode(message['value'])
    decoded_string = decoded_bytes.decode("utf-8")
    print("[MESSAGE] "+decoded_string)
    
print("Next Cursor for calling: ")
print(next_cursor)

