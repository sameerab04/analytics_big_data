import os
import email
from starbase import Connection

def process_email_table(data_dir, table_name='omo6093_email_table', port=20550):
    # Set up connection
    try:
        c = Connection(port=port)
        print("Connection established.")
    except Exception as e:
        print(f"Failed to establish connection: {e}")
        return

    try:
        t = c.table(table_name)
        
        # Drop the table if it already exists to avoid conflicts
        if t.exists():
            t.drop()
            print("Existing table dropped.")

        # Create table with necessary columns
        t.create('info')
    except Exception as e:
        print(f"Error in table creation: {e}")
        return

    for folder in os.listdir(data_dir):
        folder_path = os.path.join(data_dir, folder)
        if os.path.isdir(folder_path):
            for file in os.listdir(folder_path):
                key = os.path.join(folder, file)
                try:
                    with open(os.path.join(folder_path, file), 'r') as f:
                        email_data = f.read()
                    
                    msg = email.message_from_string(email_data)
                    to_field = msg['to'].replace('\n', '').replace('\t', '') if msg['to'] else 'unknown'
                    date_field = msg['date'] if msg['date'] else 'unknown'
                    from_field = msg['from'] if msg['from'] else 'unknown'
                    # Get fields necessary for homework
                    body_field = msg.get_payload()

                    t.insert(key, {'info': {'to': to_field, 'date': date_field, 'from': from_field, 'body': body_field}})
                    print(f"Inserted {key}")
                except Exception as e:
                    print(f"Failed to process {key}: {e}")
                    continue

if __name__ == "__main__":
    data_directory = '/home/public/enron/'
    process_email_table(data_directory)
