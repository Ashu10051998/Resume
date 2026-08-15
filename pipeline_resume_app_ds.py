import streamlit as st
import time
import pandas as pd

# Set page configuration for a professional, data-centric dashboard
st.set_page_config(
    page_title="Ashutosh Kumar | ML Pipeline Resume",
    page_icon="🧪",
    layout="wide"
)

# Custom styling to make it feel like an actual MLflow / Databricks Model Registry UI
st.markdown("""
<style>
    .reportview-container {
        background-color: #0E1117;
    }
    .pmo-header {
        background: linear-gradient(135deg, #065F46 0%, #022C22 100%);
        padding: 20px;
        border-radius: 8px;
        color: white;
        margin-bottom: 25px;
        border-bottom: 4px solid #34D399;
    }
    .dag-node-active {
        background-color: #064E3B;
        border: 2px solid #34D399;
        border-radius: 8px;
        padding: 12px;
        text-align: center;
        color: #34D399;
        font-weight: bold;
        box-shadow: 0 0 10px rgba(52, 211, 153, 0.2);
        min-height: 280px;
    }
    .dag-node-inactive {
        background-color: #111827;
        border: 2px dashed #4B5563;
        border-radius: 8px;
        padding: 12px;
        text-align: center;
        color: #9CA3AF;
        min-height: 280px;
    }
    .dag-node-success {
        background-color: #064E3B;
        border: 2px solid #059669;
        border-radius: 8px;
        padding: 12px;
        text-align: center;
        color: #A7F3D0;
        font-weight: bold;
        min-height: 280px;
    }
    .node-title {
        font-size: 0.8em;
        text-transform: uppercase;
        color: #9CA3AF;
        margin-bottom: 4px;
        font-weight: bold;
        text-align: center;
    }
    .terminal-container {
        background-color: #111827;
        border-radius: 6px;
        border: 1px solid #1F2937;
        font-family: 'Courier New', Courier, monospace;
        padding: 15px;
        margin-top: 15px;
    }
    .terminal-header {
        color: #34D399;
        border-bottom: 1px solid #1F2937;
        padding-bottom: 8px;
        margin-bottom: 10px;
        font-size: 0.85em;
    }
    .terminal-body {
        color: #F3F4F6;
        font-size: 0.85em;
        height: 250px;
        overflow-y: auto;
        line-height: 1.5em;
    }
    .metric-card {
        background-color: #1F2937;
        border-left: 4px solid #34D399;
        padding: 15px;
        border-radius: 4px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ──────────────────────────────────────────────────────────────
st.markdown("""
<div class='pmo-header'>
    <h1 style='margin:0; font-size:2em;'>🧪 ML_EXPERIENCE_PIPELINE_ORCHESTRATOR v3.0</h1>
    <p style='margin:5px 0 0 0; color:#34D399; font-family:monospace;'>
        Target: Ashutosh_Kumar_Data_Scientist_Resume // Model: Predictive_Yield_Optimizer // Status: Active_Listener
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
### 🏗️ Live End-to-End ML Lifecycle & Interactive Resume Simulation
Hiring managers shouldn't just read a static list of models—they should execute the **Machine Learning Pipeline** that standardizes and serves them. 
This interactive application simulates an **End-to-End MLOps Pipeline** based on Ashutosh's real-world data science experience. Click the button below to load the raw parameters, process noisy sensor inputs, optimize hyperparameter metrics, and write the conformed **Gold Data Scientist Resume Layer**.
""")

# ── SIDEBAR CONFIGURATION (ML CONFIGS) ──────────────────────────────────
st.sidebar.markdown("## ⚙️ ML Pipeline Configuration")

ml_orchestrator = st.sidebar.selectbox(
    "1. Choose ML Workflow Orchestrator",
    ["MLflow Registry", "Azure Machine Learning", "Kubeflow Pipelines"]
)

model_type = st.sidebar.selectbox(
    "2. Algorithm & Model Type",
    ["Random Forest Regressor", "XGBoost Regressor", "LSTM Time-Series Network"]
)

evaluation_metric = st.sidebar.selectbox(
    "3. Core Model Optimization Metric",
    ["F2 Score (Prioritize Recall)", "F1 Score (Balanced)", "Precision Only"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Current Spark ML Cluster Stats:**
*   **ML Cluster Instance:** Standard_DS4_v2 (28 GB, 8 Cores)
*   **GPU Accelerated:** NVIDIA Tesla T4 Active
*   **Frameworks:** PySpark MLlib, Scikit-learn, MLflow, Pandas
""")

# ── STEP 1: DEFINE THE 7-STAGE PIPELINE GRAPH (VISUAL DAG) ──────────────
st.markdown("### 🗺️ End-to-End Machine Learning Pipeline (Experience Workflow)")

# Define columns representing the 7 stages of app.2.png adapted for Data Science
cols = st.columns(7)

# Helper function to render a card with experience title above and description inside
def render_dag_card(col, status, title, subtitle, exp_title, exp_text):
    node_class = "dag-node-active" if status == "active" else ("dag-node-success" if status == "success" else "dag-node-inactive")
    with col:
        st.markdown(f"<div class='node-title'>{exp_title}</div>", unsafe_allow_html=True)
        st.markdown(f"""
        <div class='{node_class}'>
            <div style='font-size: 1.1em; margin-bottom: 6px;'>{title}</div>
            <div style='font-size: 0.75em; color: #9CA3AF; font-weight: normal; margin-bottom: 8px;'>{subtitle}</div>
            <div style='font-size: 0.8em; text-align: left; font-weight: normal; line-height: 1.35em; border-top: 1px solid #4B5563; padding-top: 8px;'>
                {exp_text}
            </div>
        </div>
        """, unsafe_allow_html=True)

# Define experiences mapped onto the 7 steps
exp_texts = [
    "<b>Collaborated with Acrosoft</b> and plant teams to establish database connections to physical brewing databases and historian raw transactional logs.",
    "<b>Cleaned noisy telemetry sensor streams</b> using PySpark MLlib, executing outlier handling, filtering, and data quality boundary tests on physical sensors.",
    "<b>Aggregated high-frequency raw telemetry</b> into 120-minute averaged intervals, aligning data records directly with physical brewing vessel dynamics.",
    "<b>Developed and trained predictive regressor models</b> on conformed time-series features to forecast Lauter Tun wort yields in production.",
    "<b>Custom-tuned model decision boundaries</b> using Scikit-Learn to maximize the F2 score, strategically biasing the model to prioritize recall over precision.",
    "<b>Successfully deployed models</b> into live manufacturing plant production workflows, integrating predictions into the active Control Room operations.",
    "<b>Built global reliability dashboards in Power BI</b> using custom DAX measures, translating raw machine data into standardized KPIs for global executives."
]

# Initial draw: Stage 1 is active, others inactive
render_dag_card(cols[0], "active", "🌐 Raw Sources", "Historian & SQL", "1. Upstream Sourcing", exp_texts[0])
render_dag_card(cols[1], "inactive", "🧹 Signal Processing", "Outliers & Noise", "2. Data Cleaning", exp_texts[1])
render_dag_card(cols[2], "inactive", "⏱️ Feature Engineering", "120-Min Averages", "3. Feature Extraction", exp_texts[2])
render_dag_card(cols[3], "inactive", "🤖 Model Training", "Regression Models", "4. Model Development", exp_texts[3])
render_dag_card(cols[4], "inactive", "🎯 F2 Optimization", "Prioritize Recall", "5. Model Tuning", exp_texts[4])
render_dag_card(cols[5], "inactive", "🚀 Model Deployment", "Operational Control", "6. Production Rollout", exp_texts[5])
render_dag_card(cols[6], "inactive", "📊 Stakeholder Delivery", "Power BI & DAX", "7. Executive Serving", exp_texts[6])

# ── TRIGGER PIPELINE BUTTON ────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
trigger_col, stats_col = st.columns([1, 2])

with trigger_col:
    st.markdown("### 🚀 Execute Pipeline")
    trigger_btn = st.button("Trigger Job: Execute_ML_Lifecycle()", type="primary", use_container_width=True)

with stats_col:
    st.markdown("### 📊 Pipeline Performance Metrics")
    m_col1, m_col2, m_col3 = st.columns(3)
    with m_col1:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#9CA3AF;'>PRODUCTION YIELD IMPROVEMENT</span><br><b style='color:#34D399; font-size:1.5em;'>+0.6% 📈</b></div>", unsafe_allow_html=True)
    with m_col2:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#9CA3AF;'>TUNED OPTIMIZATION TARGET</span><br><b style='color:#34D399; font-size:1.5em;'>F2 Score Bias</b></div>", unsafe_allow_html=True)
    with m_col3:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#9CA3AF;'>DATA INTEGRITY GATES</span><br><b style='color:#34D399; font-size:1.5em;'>100% Verified ✅</b></div>", unsafe_allow_html=True)

# ── PIPELINE SIMULATION EXECUTION ──────────────────────────────────────
if trigger_btn:
    st.markdown("### 💻 MLflow Run & PySpark SparkSession Console Log")
    
    # Set up container
    terminal_placeholder = st.empty()
    logs = []
    
    def run_log(step_text, sleep_time=0.4):
        logs.append(step_text)
        log_html = "".join([f"<div style='margin-bottom: 5px;'>{log}</div>" for log in logs])
        terminal_placeholder.markdown(f"""
        <div class='terminal-container'>
            <div class='terminal-header'>⚡ MLflow Run @ Node-Master // Job_ID: ML-ASHUTOSH-DS-042 // Run_Name: {ml_orchestrator.upper().replace(" ", "_")}</div>
            <div class='terminal-body'>{log_html}</div>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(sleep_time)

    # 1. Ingestion
    run_log("INFO  [MLflow] Initializing Active Run 'Lauter_Tun_Wort_Yield_Optimizer'...")
    run_log(f"INFO  [SparkSession] Instantiating PySpark session with GPU-Acceleration enabled. Algorithm: {model_type}")
    run_log("INFO  [Reader] Establishing database connection locks to physical brewing and historian transaction databases...")
    run_log("INFO  [Reader] Sourcing raw SQL databases via Acrosoft IT replication boundaries.")
    
    # Update DAG Visual 1 -> 2
    cols[0].empty()
    cols[1].empty()
    render_dag_card(cols[0], "success", "🌐 Raw Sources", "Historian & SQL", "1. Upstream Sourcing", exp_texts[0])
    render_dag_card(cols[1], "active", "🧹 Signal Processing", "Outliers & Noise", "2. Data Cleaning", exp_texts[1])
    
    # 2. Cleaning
    run_log("INFO  [PreProcessing] Ingesting raw industrial historian sensor logs. Rows loaded: 1.2M records.")
    run_log("INFO  [PreProcessing] Running signal-processing checks: smoothing high-frequency industrial noise using exponential moving averages.")
    run_log("INFO  [PreProcessing] Performing outlier detection using rolling standard-deviation constraints.")
    run_log("INFO  [PreProcessing] Handling null data arrays across critical temperature and pressure parameters.")
    
    # Update DAG Visual 2 -> 3
    cols[1].empty()
    cols[2].empty()
    render_dag_card(cols[1], "success", "🧹 Signal Processing", "Outliers & Noise", "2. Data Cleaning", exp_texts[1])
    render_dag_card(cols[2], "active", "⏱️ Feature Engineering", "120-Min Averages", "3. Feature Extraction", exp_texts[2])
    
    # 3. Feature Engineering
    run_log("INFO  [FeatureEngine] Initiating batch-wise process telemetry aggregations...")
    run_log("INFO  [FeatureEngine] Resampling raw, high-frequency signals into 120-minute averaged intervals.")
    run_log("INFO  [FeatureEngine] -- Rationale: Aligning telemetry records directly with the physics of 120-minute brewing vessel cycles.")
    run_log("INFO  [FeatureEngine] Writing optimized feature array to Databricks Feature Store table: `db_features.lauter_tun_conformed`")
    
    # Update DAG Visual 3 -> 4
    cols[2].empty()
    cols[3].empty()
    render_dag_card(cols[2], "success", "⏱️ Feature Engineering", "120-Min Averages", "3. Feature Extraction", exp_texts[2])
    render_dag_card(cols[3], "active", "🤖 Model Training", "Regression Models", "4. Model Development", exp_texts[3])
    
    # 4. Model Training
    run_log(f"INFO  [ModelDevelopment] Fetching standardized feature set from Feature Store.")
    run_log(f"INFO  [ModelDevelopment] Initializing distributed Scikit-learn {model_type} algorithm.")
    run_log("INFO  [ModelDevelopment] Running 5-fold cross-validation grid search to isolate optimal parameters...")
    run_log("INFO  [ModelDevelopment] Successfully isolated optimal regressor tree boundaries.")
    
    # Update DAG Visual 4 -> 5
    cols[3].empty()
    cols[4].empty()
    render_dag_card(cols[3], "success", "🤖 Model Training", "Regression Models", "4. Model Development", exp_texts[3])
    render_dag_card(cols[4], "active", "🎯 F2 Optimization", "Prioritize Recall", "5. Model Tuning", exp_texts[4])
    
    # 5. Optimization
    run_log(f"INFO  [TuningEngine] Evaluating baseline validation parameters. Baseline Recall: 0.72 // Precision: 0.81")
    run_log(f"INFO  [TuningEngine] Customizing model optimization objective to prioritize **{evaluation_metric}**.")
    if "F2" in evaluation_metric:
        run_log("INFO  [TuningEngine] -- Setting beta coefficient = 2 to prioritize recall over precision.")
        run_log("INFO  [TuningEngine] -- Optimization Goal: Minimizing missed low-yield events (false negatives) at the physical plant.")
        run_log("INFO  [TuningEngine] Hyperparameter search complete. Optimized F2 validation score: 0.89.")
    else:
        run_log("INFO  [TuningEngine] Standard threshold optimizations complete.")
    
    # Update DAG Visual 5 -> 6
    cols[4].empty()
    cols[5].empty()
    render_dag_card(cols[4], "success", "🎯 F2 Optimization", "Prioritize Recall", "5. Model Tuning", exp_texts[4])
    render_dag_card(cols[5], "active", "🚀 Model Deployment", "Operational Control", "6. Production Rollout", exp_texts[5])
    
    # 6. Deployment
    run_log("INFO  [DeploymentPipeline] Initiating model artifact packaging inside Docker container.")
    run_log("INFO  [DeploymentPipeline] Validating model performance indicators against production baseline standard.")
    run_log("INFO  [DeploymentPipeline] Registering model inside MLflow Model Registry as: `db_models.lauter_tun_yield_optimizer:v1.0.1`")
    run_log("INFO  [DeploymentPipeline] Deploying model endpoints to live manufacturing control system APIs.")
    run_log("<span style='color:#34D399; font-weight:bold;'>SUCCESS [DeploymentPipeline] Model successfully integrated into active plant control room workflow!</span>")
    
    # Update DAG Visual 6 -> 7
    cols[5].empty()
    cols[6].empty()
    render_dag_card(cols[5], "success", "🚀 Model Deployment", "Operational Control", "6. Production Rollout", exp_texts[5])
    render_dag_card(cols[6], "active", "📊 Stakeholder Delivery", "Power BI & DAX", "7. Executive Serving", exp_texts[6])
    
    # 7. Serving & BI
    run_log("INFO  [BI_Serving] Aggregating model predictions and writing batch metadata to Gold Delta Lake Layer.")
    run_log("INFO  [BI_Serving] Compiling raw maintenance records and standardizing SAP PM tables (Transactions IW37, IP28, IH24, IW47, ZI203).")
    run_log("INFO  [BI_Serving] Constructing global plant reliability and yield dashboards.")
    run_log("INFO  [BI_Serving] Engineering custom Power BI DAX formulas to serve standardized metrics to global executives.")
    run_log("<span style='color:#10B981; font-weight:bold;'>SUCCESS [MLflow] Run complete. 100% of pipeline nodes conformed and served.</span>", sleep_time=0.8)
    
    # Final Visual complete state
    cols[6].empty()
    render_dag_card(cols[6], "success", "📊 Stakeholder Delivery", "Power BI & DAX", "7. Executive Serving", exp_texts[6])
    
    st.balloons()
    st.success("🎉 Machine Learning Lifecycle complete! Conformed Gold Data Scientist Resume Layer successfully generated.")

    # ── SHOW FINALIZED RESUME DATA ─────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🥇 Gold Layer Model Table Output: `db_gold.ashutosh_kumar_ds_resume`")
    
    resume_df = pd.DataFrame([
        {"Experience Component": "Data Analyst / Scientist (Lauter Tun)", "Core Tech Stack": "Python, Machine Learning, Signal Processing", "Standardized Scope": "Signal smoothing on raw sensor logs, 120-min rolling aggregation intervals, and training regressors optimized for F2 recall.", "Business Value": "Successfully drove a +0.6% wort production yield increase in live physical manufacturing."},
        {"Experience Component": "Power BI Developer (ReliabilityGKPI)", "Core Tech Stack": "Power BI, DAX, MS Excel", "Standardized Scope": "Constructing global reliability dashboards; engineering custom DAX measures to translate raw SAP PM logs into unified KPIs.", "Business Value": "Enabled executive level tracking of equipment performance across multiple global plants."},
        {"Experience Component": "Data Engineer (BrewDat 3.0)", "Core Tech Stack": "Azure Data Factory (ADF), Databricks, PySpark, Delta Lake", "Standardized Scope": "Onboarding and standardizing core SAP PM Transactions (IW37, IP28, IH24, IW47, ZI203) across global operating zones.", "Business Value": "Eliminated manual error risks and automated previously manual reporting pipelines."}
    ])
    
    st.dataframe(resume_df, use_container_width=True)
    
    st.markdown("---")
    
    # ── FINAL RENDERED RESUME ──────────────────────────────────────────
    st.markdown("### 📄 Compiled Executive Data Scientist Resume")
    
    resume_md = """
    # ASHUTOSH KUMAR
    **Data Scientist & Machine Learning Specialist**  
    Bangalore, India | ashutoshkr1005@gmail.com | +91 79923 45808 | [linkedin.com/in/ashutoshkumar42](https://linkedin.com/in/ashutoshkumar42)  
    *Interactive App:* **[resume-lgvemnt3dck3jglnpdanmb.streamlit.app](https://resume-lgvemnt3dck3jglnpdanmb.streamlit.app)**

    ---

    ### 🛠️ Core Data Science & ML Expertise
    *   **Data Science & Machine Learning:** Statistical Modeling, Predictive Modeling, Regression, Machine Learning, Feature/Signal Processing, Hyperparameter Optimization.
    *   **Data Prep & Wrangling:** Data Cleaning, Outlier Handling, Signal Smoothing & Noise Filtering, 120-Minute Rolling Aggregations.
    *   **Distributed Processing & Cloud:** Azure Data Factory (ADF), Azure Databricks, PySpark MLlib, Delta Lake, ADLS Gen2, Spark SQL.
    *   **Enterprise Integrations:** SAP Plant Maintenance (PM) Modules (IW37, IP28, IH24, IW47, ZI203), BrewDat 3.0 Enterprise Architecture.
    *   **BI & Analytics:** Power BI, Complex DAX Measures, Calculated Tables, Enterprise Data Lineage.

    ---

    ### 💼 Professional Experience

    #### **Networth Corp — Bangalore, India** *(Data science & engineering services provider to AB InBev)*
    
    **Data Analyst — Lauter Tun Production Optimization** | *Jan 2022 – Dec 2023*
    *   **Yield Optimization (+0.6%):** Co-led a massive predictive analytics initiative to optimize the physical Lauter Tun brewing pipeline, delivering an on-target **0.6% wort production yield increase** in live manufacturing operations via machine learning models.
    *   **Telemetry Noise Filtering:** Engineered custom Python preprocessing scripts to filter noisy, high-frequency historian sensor logs, executing outlier handling and statistical signal smoothing on raw temperature and pressure telemetry.
    *   **Feature Engineering & Alignment:** Resampled high-frequency raw telemetry records into 120-minute moving-average intervals to align sensor metrics with physical brewing vessel dynamics and stabilize predictive model features.
    *   **Model Tuning ($F_2$ Recall Bias):** Custom-tuned model decision boundaries to maximize the **$F_2$ score to prioritize model recall over precision**, strategically minimizing missed low-yield events to safeguard plant efficiency.
    *   **Stakeholder Deployment:** Partnered with physical plant stakeholders to integrate predictive models into live production workflows, establishing clear feedback and model verification parameters.

    **Power BI Developer — Global Reliability KPI Dashboards** | *Jan 2024 – Oct 2024*
    *   **Analytical Metric Serving:** Built global equipment reliability dashboards in Power BI consumed by plant managers and executives to monitor asset performance.
    *   **Complex DAX Modeling:** Programmed custom DAX measures and calculated tables to transform raw transactional database records into standardized, unified global reliability KPIs.
    *   **Data Governance:** Conducted stakeholder alignment sessions to validate KPI business rules, documenting end-to-end data lineage for enterprise governance.

    **Data Engineer — ReliabilityGKPI & Maintenance One** | *Oct 2024 – Present*
    *   **Enterprise Data Onboarding:** Architected the onboarding and integration of raw SAP Plant Maintenance data (Transactions IW37, IP28, IH24, IW47, and ZI203) into the Azure Delta Lake, establishing a unified global maintenance database.
    *   **Orchestration & Automation:** Built automated, event-triggered ELT pipelines in Azure Data Factory to ingest heavy transaction records across global zones, eliminating manual refresh workflows and manual error risks.

    ---

    ### 🎓 Education
    *   **B.Tech in Engineering** | University Institute of Technology, University of Burdwan (2016 – 2020) | *Final Project: Trajectory Tracing & Control Logic of a Quadcopter (77.7%)*
    *   **SRC Inter College** (2013 – 2015) | *Physics, Chemistry, Mathematics (71.4%)*
    """
    
    st.markdown(resume_md)
    
    # Download Button
    st.download_button(
        label="📥 Download Standard Plain-Text DS Resume",
        data=resume_md,
        file_name="ashutosh_kumar_data_scientist_resume.txt",
        mime="text/plain"
    )
