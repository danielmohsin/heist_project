import json
import psycopg2
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

def parse_heist_data(file_path):

    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            heist = data.get("heist", {})
            return {
                "id": heist.get("id"),
                "started_at": heist.get("startedAt"),
                "ended_at": heist.get("endedAt"),
                "total_amount_emitted": float(heist.get("totalAmountEmitted", 0)),
                "total_amount_claimed": float(heist.get("totalAmountClaimed", 0)),
                "nft_address": heist.get("nftId"),
                "wallet_id": heist.get("walletId"),
                "location_id": heist.get("locationId"),
                "event_id": heist.get("eventId"),
                "is_bonus_roll": heist.get("isBonusRoll"),
                "amount": data.get("amount", 0)
            }
    except Exception as e:
        print(f"Error parsing data: {e}")
        return None

def insert_into_db(heist):

    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()


        insert_query = """
        INSERT INTO heists (
            id, started_at, ended_at, total_amount_emitted, total_amount_claimed,
            nft_address, wallet_id, location_id, event_id, is_bonus_roll, amount
        ) VALUES (
            %(id)s, %(started_at)s, %(ended_at)s, %(total_amount_emitted)s, %(total_amount_claimed)s,
            %(nft_address)s, %(wallet_id)s, %(location_id)s, %(event_id)s, %(is_bonus_roll)s, %(amount)s
        )
        """
        cursor.execute(insert_query, heist)
        connection.commit()
        print("Heist data inserted successfully.")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":

    file_path = "sample_data.json"
    

    heist = parse_heist_data(file_path)
    if heist:

        insert_into_db(heist)
