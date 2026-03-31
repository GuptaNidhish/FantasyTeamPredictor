import streamlit as st
from run_inference import run_inference_pipeline
from ingestion_service import ingest_latest_completed_match
from retraining import retrain_model
import json  # ✅ ADDED: to read processed_matches.json
import os    # ✅ ADDED: for retrain log file handling

# ================= RETRAIN LOG HELPERS =================
RETRAIN_LOG_FILE = "last_retrain.json"  # ✅ ADDED

def should_retrain(current_count):
    """
    ✅ ADDED:
    Ensures retraining happens ONLY once at 10, 20, 30... matches
    even if Streamlit reruns multiple times
    """
    try:
        if os.path.exists(RETRAIN_LOG_FILE):
            with open(RETRAIN_LOG_FILE, "r") as f:
                data = json.load(f)
                last_count = data.get("last_retrain_count", 0)
        else:
            last_count = 0
    except:
        last_count = 0

    if current_count % 10 == 0 and current_count != last_count:
        with open(RETRAIN_LOG_FILE, "w") as f:
            json.dump({"last_retrain_count": current_count}, f)
        return True

    return False

# ================= UI CONFIG =================
st.set_page_config(
    page_title="Dream11 Predictor",
    page_icon="🏏",
    layout="wide"
)

# ================= HEADER =================
st.title("🏏 Dream11 Fantasy Team Predictor")
st.markdown("AI-powered match predictions")

# ================= RUN INFERENCE =================
st.subheader("🔮 Predicted Team")
with st.spinner("Running AI model..."):
    result = run_inference_pipeline()

if result:
    team = result['team']
    captain = result['captain']
    vice_captain = result['vice_captain']

    st.success("✅ Prediction Ready!")

    # 👑 Captain / VC
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 👑 Captain")
        st.info(captain)
    with col2:
        st.markdown("### 🤝 Vice Captain")
        st.info(vice_captain)

    # 🏏 Playing XI
    st.markdown("### 🏏 Playing XI")
    for player in team:
        if player == captain:
            st.write(f"👑 {player}")
        elif player == vice_captain:
            st.write(f"🤝 {player}")
        else:
            st.write(f"• {player}")
else:
    st.error("❌ Prediction failed")

# ================= RUN INGESTION =================
logs = ingest_latest_completed_match()  # ✅ handles next_match.json internally
if logs:
    print("\n📥 Ingestion logs:")
    for log in logs:
        print(log)
else:
    print("⚠️ Ingestion function ran but returned no logs")

# ================= RETRAIN TRIGGER =================
# ✅ UPDATED: Safe retraining logic (only once per 10 matches)
try:
    with open("processed_matches.json", "r") as f:
        processed_matches = json.load(f)

    if isinstance(processed_matches, list):
        count = len(processed_matches)

        if should_retrain(count):  # ✅ CHANGED
            retrain_model()
            print(f"🔄 Retraining model triggered at {count} matches")
        else:
            next_trigger = ((count // 10) + 1) * 10
            print(f"ℹ️ Current matches: {count} | Next retrain at {next_trigger}")

except FileNotFoundError:
    print("⚠️ processed_matches.json not found")
except Exception as e:
    print(f"⚠️ Error while checking retrain condition: {e}")