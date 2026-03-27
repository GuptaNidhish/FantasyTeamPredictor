import streamlit as st
from run_inference import run_inference_pipeline
from ingestion_service import ingest_latest_completed_match

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