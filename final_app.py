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

# ================= SIDEBAR =================
st.sidebar.header("⚙️ Controls")

run_prediction = st.sidebar.button("🔮 Run Prediction")
refresh_data = st.sidebar.button("📥 Run Ingestion")

# ================= MAIN =================
if run_prediction:
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
            st.markdown(f"### 👑 Captain")
            st.info(captain)

        with col2:
            st.markdown(f"### 🤝 Vice Captain")
            st.info(vice_captain)

        # 🏏 Team
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

# ================= INGESTION =================
if refresh_data:
    st.subheader("📥 Data Ingestion")

    with st.spinner("Fetching latest match data..."):
        logs = ingest_latest_completed_match()  # ✅ capture logs

    # ✅ Display logs properly
    if logs:
        for log in logs:
            if "❌" in log:
                st.error(log)
            elif "⚠️" in log:
                st.warning(log)
            else:
                st.success(log)
    else:
        st.info("No logs returned")