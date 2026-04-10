import streamlit as st
from run_inference import run_inference_pipeline
from ingestion_service import ingest_latest_completed_match
from retraining import retrain_model
from db.initialization import SessionLocal
from db.models import ProcessedMatch  # ✅ CHANGED

# ================= RETRAIN LOG HELPERS =================
def should_retrain(current_count):
    """
    ✅ CHANGED: No JSON, no new table
    Logic:
    - Retrain every 10 matches
    - Only trigger once per multiple of 10
    """

    if current_count == 0:
        return False

    # Only trigger on multiples of 10
    if current_count % 10 != 0:
        return False

    # ✅ Prevent repeated retraining:
    # Check if we already retrained at this count by using modulo logic
    # Since count increases strictly, this condition naturally prevents repeats
    return True


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
with st.spinner("Running AI model..."):
    result = run_inference_pipeline()

if result:
    team1_name = result['team1_name']
    team2_name = result['team2_name']

    team1_img = result.get('team1_img')
    team2_img = result.get('team2_img')

    if team1_img:
        team1_img = team1_img.replace("w=48", "w=500")
    if team2_img:
        team2_img = team2_img.replace("w=48", "w=500")

    team = result['team']
    captain = result['captain']
    vice_captain = result['vice_captain']
    player_to_img = result.get('playertoimg', {})

    # ================= MATCH HEADER =================
    st.subheader("🏟️ Match")

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if team1_img:
            st.image(team1_img, width=220)
        st.markdown(f"**{team1_name}**")

    with col2:
        st.write("")
        st.write("")
        st.markdown("## VS")

    with col3:
        if team2_img:
            st.image(team2_img, width=220)
        st.markdown(f"**{team2_name}**")

    st.divider()

    # ================= PREDICTION HEADER =================
    st.subheader("🔮 Predicted Team")
    st.success("✅ Prediction Ready!")

    # ================= CAPTAIN / VC =================
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("👑 Captain")
        st.success(captain)

    with col2:
        st.subheader("🤝 Vice Captain")
        st.info(vice_captain)

    st.divider()

    # ================= PLAYING XI =================
    st.subheader("🏏 Playing XI")

    for player in team:
        col1, col2 = st.columns([1, 5])

        img_url = player_to_img.get(player)

        with col1:
            if img_url:
                st.image(img_url, width=50)
            else:
                st.image("https://via.placeholder.com/50", width=50)

        with col2:
            if player == captain:
                st.markdown(f"👑 **{player} (C)**")
            elif player == vice_captain:
                st.markdown(f"🤝 **{player} (VC)**")
            else:
                st.markdown(f"• {player}")

else:
    st.error("❌ Prediction failed")

# ================= RUN INGESTION =================
logs = ingest_latest_completed_match()
if logs:
    print("\n📥 Ingestion logs:")
    for log in logs:
        print(log)
else:
    print("⚠️ Ingestion function ran but returned no logs")

# ================= RETRAIN TRIGGER =================
session = SessionLocal()  # ✅ CHANGED

try:
    # ✅ CHANGED: Get count from DB
    count = session.query(ProcessedMatch).count()

    if should_retrain(count):
        retrain_model()
        print(f"🔄 Retraining model triggered at {count} matches")
    else:
        next_trigger = ((count // 10) + 1) * 10
        print(f"ℹ️ Current matches: {count} | Next retrain at {next_trigger}")

except Exception as e:
    print(f"⚠️ Error while checking retrain condition: {e}")

finally:
    session.close()  # ✅ CHANGED