import json
from db.initialization import SessionLocal
from db.models import ProcessedMatch, NextMatch

db = SessionLocal()

# ================= PROCESSED MATCHES =================
try:
    with open("processed_matches.json", "r") as f:
        processed_ids = json.load(f)

    for match_id in processed_ids:
        exists = db.query(ProcessedMatch).filter_by(match_id=match_id).first()
        if not exists:
            db.add(ProcessedMatch(match_id=match_id))

    db.commit()
    print("✅ Processed matches migrated")

except FileNotFoundError:
    print("⚠️ processed_matches.json not found")
except Exception as e:
    print("⚠️ Error migrating processed_matches:", e)


# ================= NEXT MATCHES =================
try:
    with open("next_match.json", "r") as f:
        matches = json.load(f)

    # Ensure it's a list
    if not isinstance(matches, list):
        raise Exception("next_match.json is not a list")

    for data in matches:
        # Skip invalid entries
        if "id" not in data:
            continue

        existing = db.query(NextMatch).filter_by(id=data["id"]).first()

        if not existing:
            new_match = NextMatch(
                id=data["id"],
                team1=data.get("team1"),
                team2=data.get("team2"),
                venue=data.get("venue"),
                date=data.get("date"),
                img1=data.get("img1"),
                img2=data.get("img2")
            )
            db.add(new_match)

    db.commit()
    print("✅ Next matches migrated")

except FileNotFoundError:
    print("⚠️ next_match.json not found")
except Exception as e:
    print("⚠️ Error migrating next_match:", e)


# ================= CLEANUP =================
db.close()
print("🚀 Migration completed")