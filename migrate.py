from concurrent.futures import ThreadPoolExecutor, as_completed
from db_manager import DatabaseManager
from conversation import ConversationAPI

db = DatabaseManager()
api = ConversationAPI()

def process_conversation(conversation: dict):
    conversation_id = conversation["conversationId"]
    resp = api.get_conversation_info(conversation_id)
    db.mark_conversation_as_processed(conversation_id)
    if not resp.get('messages') or len(resp['messages']) <= 0:
        res = api.update_conversation(conversation_id, conversation["messages"])
        print(f"Response for conversation {conversation_id}: {res}")
    

def migrate_one_user(user_id: str):
    conversations = db.get_user_conversations(user_id)
    conversations = convert_format(conversations)
    total_convs = len(conversations)
    print(f"Processing {total_convs} conversations for user {user_id}")
    
    with ThreadPoolExecutor(max_workers=5) as conv_executor:
        futures = [conv_executor.submit(process_conversation, conv) for conv in conversations]
        completed = 0
        for future in as_completed(futures):
            try:
                future.result()
                completed += 1
                if completed % 10 == 0:  # 每处理10个会话显示一次进度
                    print(f"User {user_id}: Processed {completed}/{total_convs} conversations")
            except Exception as exc:
                print(f"Error processing conversation: {exc}")
    
    print(f"Completed processing user {user_id}")
    db.mark_user_as_migrated(user_id)

def convert_format(raw: list) -> list:
    result = []
    for conversation in raw:
        conversation_id = conversation["conversationId"]
        messages = conversation["messages"]
        new_messages = list(map(lambda message: {"messageId": message["id"], "messageData": message}, messages))
        result.append({"conversationId": conversation_id, "messages": new_messages})
    return result

if __name__ == "__main__":
    max_workers = 10
    batch_size = 10
    offset = 0
    
    while True:
        users = db.get_users(batch_size, offset)
        if not users:
            break
            
        print(f"Processing batch of users from offset {offset}")
        with ThreadPoolExecutor(max_workers=max_workers) as user_executor:
            futures = [user_executor.submit(migrate_one_user, user[0]) for user in users]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"Error processing user: {exc}")
        
        offset += batch_size
        print(f"Completed batch, moving to offset {offset}")