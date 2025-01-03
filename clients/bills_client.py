

import json
import os
import time
import logging
from typing import List, Dict

import requests
from token_associations import TOKENS


class BillsAPIClient:
    """Bills API client."""
    endpoint = os.getenv('BILLS_ENDPOINT', 'https://dev-bills.bukovel.net/api/v1/bills/cart-1')

    def match_tokens(self, parking_name: str) -> str:
        return TOKENS.get(parking_name, '')

    def make_headers(self, token: str) -> Dict:
        return {
            'Content-Type': 'application/json',
            'Authorization': token
        }

    def send(self, objects: List[Dict]) -> List[Dict]:
        """Send batch data to Bills with retry logic."""
        response_data = []
        for obj in objects:
            try:
                ID = obj.pop('ID')
                OPERATION_ID = obj.pop('OPERATION_ID')

                parking_name = obj.pop('POINT_OF_SALE')
                token = self.match_tokens(parking_name)

                response = requests.post(
                    url=self.endpoint,
                    json=obj,
                    headers=self.make_headers(token),
                )
                
                if response.status_code == 201:
                    rdata = response.json()['data']
                    logging.info(f"Successfully sent data to Bills: {rdata['id']}, {rdata['document_id']}, {rdata['date_payment']}")

                    obj['ID'] = ID
                    obj['OPERATION_ID'] = OPERATION_ID
                    
                    response_data.append(obj)
                else:
                    logging.error(f"Failed to send data to Bills: {response.json()}, Object: {obj}")
                    time.sleep(2)

            except Exception as e:
                logging.error(f"Failed to send data to Bills: {e}, Object: {obj}")
                time.sleep(2)

        return response_data
