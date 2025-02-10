import requests
import json

class ConversationAPI:
    def __init__(self, base_url="http://localhost:8081"):
        self.base_url = base_url
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.session = requests.session()

    def get_conversation_info(self, business_id, cursor=None, length=10, all_main_data=True):
        payload = {
            "businessId": business_id,
            "businessType": "conversation",
            "cursor": cursor,
            "length": length,
            "allMainData": all_main_data
        }
        
        response = self.session.post(
            f"{self.base_url}/info",
            headers=self.headers,
            json=payload
        )
        return response.json()

    def update_conversation(self, business_id, messages):
        batch_size = 1000
        if len(messages) <= batch_size:
            payload = {
                "businessId": business_id,
                "businessType": "conversation",
                "messages": messages
            }
            
            response = self.session.post(
                f"{self.base_url}/update",
                headers=self.headers,
                json=payload
            )
            return response.json()
        
        results = []
        for i in range(0, len(messages), batch_size):
            batch_messages = messages[i:i + batch_size]
            payload = {
                "businessId": business_id,
                "businessType": "conversation",
                "messages": batch_messages
            }
            
            response = self.session.post(
                f"{self.base_url}/update",
                headers=self.headers,
                json=payload
            )
            results.append(response.json())
        
        return results