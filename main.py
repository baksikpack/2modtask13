from app.connector import connector
from pprint import pprint

if __name__ == '__main__':
    if not connector.table_users_exists():
        print("Create users table")
        connector.create_table_users()

    if not connector.table_ads_exists():
        print("Create ads table")
        connector.create_table_ads()

    connector.clear()

    user1 = connector.add_user("user1", "strong_password1")
    user2 = connector.add_user("user2", "strong_password2")

    connector.add_ad(user1, "message1 user1")
    connector.add_ad(user2, "message2 user2")
    connector.add_ad(user2, "message3 user2")

    soft_delete = [
        connector.add_ad(user1, "message soft delete user1"),
        connector.add_ad(user2, "message soft delete user2"),
    ]

    hard_delete = [
        connector.add_ad(user1, "message soft delete user1"),
        connector.add_ad(user2, "message soft delete user2"),
    ]

    ads = connector.all_ads()
    print("Before delete Ads:", len(ads))
    pprint(ads)

    for pk in soft_delete:
        connector.soft_delete_ad(pk)
    for pk in hard_delete:
        connector.delete_ad(pk)

    ads = connector.all_ads()
    print("After delete Ads:", len(ads))
    pprint(ads)
