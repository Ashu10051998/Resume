import streamlit as st
import time
import pandas as pd

# Set page configuration for a professional, data-centric dashboard
st.set_page_config(
    page_title="Ashutosh Kumar | Data Pipeline Resume",
    page_icon="⚡",
    layout="wide"
)

# Custom styling to make it feel like an actual Data Engineering Orchestrator / Airflow UI
st.markdown("""
<style>
    .reportview-container {
        background-color: #0E1117;
    }
    .pmo-header {
        background: linear-gradient(135deg, #1F3A60 0%, #0F1E36 100%);
        padding: 20px;
        border-radius: 8px;
        color: white;
        margin-bottom: 25px;
        border-bottom: 4px solid #00D2FF;
    }
    .dag-node-active {
        background-color: #1E293B;
        border: 2px solid #00FFCC;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        color: #00FFCC;
        font-weight: bold;
        box-shadow: 0 0 10px rgba(0, 255, 204, 0.2);
    }
    .dag-node-inactive {
        background-color: #1E293B;
        border: 2px dashed #475569;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        color: #94A3B8;
    }
    .dag-node-success {
        background-color: #0F172A;
        border: 2px solid #10B981;
        border-radius: 8px;
        padding: 15px;
        text-align: center;
        color: #10B981;
        font-weight: bold;
    }
    .terminal-container {
        background-color: #0F172A;
        border-radius: 6px;
        border: 1px solid #1E293B;
        font-family: 'Courier New', Courier, monospace;
        padding: 15px;
        margin-top: 15px;
    }
    .terminal-header {
        color: #38BDF8;
        border-bottom: 1px solid #1E293B;
        padding-bottom: 8px;
        margin-bottom: 10px;
        font-size: 0.85em;
    }
    .terminal-body {
        color: #E2E8F0;
        font-size: 0.85em;
        height: 250px;
        overflow-y: auto;
        line-height: 1.5em;
    }
    .metric-card {
        background-color: #1E293B;
        border-left: 4px solid #38BDF8;
        padding: 15px;
        border-radius: 4px;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ──────────────────────────────────────────────────────────────
st.markdown("""
<div class='pmo-header'>
    <h1 style='margin:0; font-size:2em;'>⚡ CAREER_PIPELINE_ORCHESTRATOR v2.0</h1>
    <p style='margin:5px 0 0 0; color:#38BDF8; font-family:monospace;'>
        Target: Ashutosh_Kumar_Resume // Environment: Production_Delta_Lake // Status: Active_Listener
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
### 🏗️ Live Data Pipeline & ETL Resume Simulation
Hiring managers shouldn't just read a static document—they should run the pipeline that builds it. 
This interactive application simulates an **Azure Data Factory & Databricks Medallion Pipeline** that ingests raw, unstructured career logs, executes strict schema standardizations, features-engineers industrial yields, and writes a finalized **Gold Resume Layer** to an optimized Delta Lake store.
""")

# ── SIDEBAR CONFIGURATION (PIPELINE CONFIGS) ───────────────────────────
st.sidebar.markdown("## ⚙️ Pipeline Configuration")

orchestrator = st.sidebar.selectbox(
    "1. Choose Orchestrator Engine",
    ["Azure Data Factory (ADF)", "Apache Airflow / DAG", "Databricks Workflows"]
)

compute_engine = st.sidebar.selectbox(
    "2. Distributed Compute Engine",
    ["Apache Spark / PySpark", "Databricks Delta Live Tables (DLT)", "Azure Synapse Spark"]
)

compression_codec = st.sidebar.selectbox(
    "3. Gold Table Compression Codec",
    ["Snappy (Parquet Default)", "GZIP", "ZSTD", "Uncompressed"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Current Cluster Stats:**
*   **Driver Type:** Standard_D4s_v5 (16 GB, 4 Cores)
*   **Worker Nodes:** Auto-scaling (2 to 8 nodes)
*   **Storage Account:** ADLS Gen2 (Hierarchical Namespace Enabled)
""")

# ── STEP 1: DEFINE THE PIPELINE GRAPH (VISUAL DAG) ─────────────────────
st.markdown("### 🗺️ Pipeline DAG (Directed Acyclic Graph) View")

# Define columns to represent the Medallion steps
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown("""
    <div class='dag-node-active'>
        📥 INGESTION LAYER<br>
        <span style='font-size:0.75em; font-weight:normal; color:#E2E8F0;'>Raw CSV, SAP PM, and Time-Series Logs</span>
    </div>
    """, unsafe_allow_html=True)

with c2:
    st.markdown("""
    <div class='dag-node-inactive' id='bronze-node'>
        🟫 BRONZE LAYER<br>
        <span style='font-size:0.75em;'>Append-Only Delta Table (Raw Ingest)</span>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown("""
    <div class='dag-node-inactive' id='silver-node'>
        🥈 SILVER LAYER<br>
        <span style='font-size:0.75em;'>Enforce Schema & SAP PM standardizations</span>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown("""
    <div class='dag-node-inactive' id='gold-node'>
        🥇 GOLD LAYER<br>
        <span style='font-size:0.75em;'>Business Aggregations & Metric Outputs</span>
    </div>
    """, unsafe_allow_html=True)

# ── TRIGGER PIPELINE BUTTON ────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
trigger_col, stats_col = st.columns([1, 2])

with trigger_col:
    st.markdown("### 🚀 Execute Pipeline")
    trigger_btn = st.button("Trigger Job: Build_Gold_Resume()", type="primary", use_container_width=True)

with stats_col:
    st.markdown("### 📊 Pipeline Metrics (Output Preview)")
    m_col1, m_col2, m_col3 = st.columns(3)
    with m_col1:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#94A3B8;'>RAW ROWS PROCESSED</span><br><b>142,500</b></div>", unsafe_allow_html=True)
    with m_col2:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#94A3B8;'>ETL RUNTIME</span><br><b>4.2s (Simulated)</b></div>", unsafe_allow_html=True)
    with m_col3:
        st.markdown("<div class='metric-card'><span style='font-size:0.8em; color:#94A3B8;'>DATA INTEGRITY GATES</span><br><b style='color:#10B981;'>100% Passed ✅</b></div>", unsafe_allow_html=True)

# ── PIPELINE SIMULATION EXECUTION ──────────────────────────────────────
if trigger_btn:
    st.markdown("### 💻 Real-Time PySpark Console Log")
    
    # Set up container
    terminal_placeholder = st.empty()
    logs = []
    
    def run_log(step_text, sleep_time=0.4):
        logs.append(step_text)
        log_html = "".join([f"<div style='margin-bottom: 5px;'>{log}</div>" for log in logs])
        terminal_placeholder.markdown(f"""
        <div class='terminal-container'>
            <div class='terminal-header'>⚡ PySpark @ Driver-Node [{orchestrator.upper().replace(" ", "_")}] // Job_ID: JOB-ASHUTOSH-DE-001</div>
            <div class='terminal-body'>{log_html}</div>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(sleep_time)

    # 1. Ingestion
    run_log("INFO  [JobTracker] Initializing SparkSession with Delta Lake support enabled.")
    run_log(f"INFO  [JobTracker] Using compression codec: {compression_codec.upper()}. compute_engine: {compute_engine}")
    run_log("INFO  [Reader] Scanning Azure Data Lake Gen2 storage container 'raw-career-data'...")
    run_log("INFO  [Reader] Ingesting files: ['academic_records.csv', 'industrial_sensor_logs.parquet', 'sap_pm_trans.json']")
    run_log("INFO  [Reader] Initial ingestion complete. Volume size: 142,500 records.")
    
    # Update DAG Visual 1 -> 2
    c1.markdown("<div class='dag-node-success'>📥 INGESTION LAYER<br><span style='font-size:0.75em; font-weight:normal;'>Completed</span></div>", unsafe_allow_html=True)
    c2.markdown("<div class='dag-node-active'>🟫 BRONZE LAYER<br><span style='font-size:0.75em; font-weight:normal; color:#E2E8F0;'>Raw Appends & Key Deduplications</span></div>", unsafe_allow_html=True)
    
    # 2. Bronze Processing
    run_log("INFO  [BronzeWriter] Writing raw appends to Delta Lake bronze table: `db_bronze.raw_career_history`")
    run_log("INFO  [BronzeWriter] Ingestion complete. Performing record level deduplications based on surrogate keys.")
    run_log("INFO  [BronzeWriter] Successfully handled 1,420 duplicate records. Validation threshold checks passed.")
    
    # Update DAG Visual 2 -> 3
    c2.markdown("<div class='dag-node-success'>🟫 BRONZE LAYER<br><span style='font-size:0.75em; font-weight:normal;'>Completed</span></div>", unsafe_allow_html=True)
    c3.markdown("<div class='dag-node-active'>🥈 SILVER LAYER<br><span style='font-size:0.75em; font-weight:normal; color:#E2E8F0;'>Schema Conformance & Standardizations</span></div>", unsafe_allow_html=True)
    
    # 3. Silver Processing
    run_log("INFO  [SilverTransform] Loading Delta table 'db_bronze.raw_career_history' into Spark Dataframe.")
    run_log("INFO  [SilverTransform] Casting raw timestamps, cleaning null values, and parsing nested JSON payloads.")
    run_log("INFO  [SilverTransform] Mapping legacy transaction codes to standardized SAP Plant Maintenance schemas:")
    run_log("INFO  [SilverTransform] -- Identified SAP PM Transactions: IW37, IP28, IH24, IW47, ZI203.")
    run_log("INFO  [SilverTransform] -- Standardizing data flow for Global Zonal coverage (AFR, EUR, MAZ, APAC, SAZ, NAZ).")
    run_log("INFO  [SilverTransform] Re-indexing and auditing structural schemas...")
    run_log("INFO  [SilverTransform] Writing clean conformed records to parquet backended table: `db_silver.conformed_career_history`")
    
    # Update DAG Visual 3 -> 4
    c3.markdown("<div class='dag-node-success'>🥈 SILVER LAYER<br><span style='font-size:0.75em; font-weight:normal;'>Completed</span></div>", unsafe_allow_html=True)
    c4.markdown("<div class='dag-node-active'>🥇 GOLD LAYER<br><span style='font-size:0.75em; font-weight:normal; color:#E2E8F0;'>Aggregating & KPI Features Extraction</span></div>", unsafe_allow_html=True)
    
    # 4. Gold Processing
    run_log("INFO  [GoldTransform] Calculating business metrics & advanced feature extractions.")
    run_log("INFO  [GoldTransform] Running signal processing aggregations on Lauter Tun industrial sensor logs:")
    run_log("INFO  [GoldTransform] -- Aggregating high-frequency sensor readings into 120-minute averaged intervals.")
    run_log("INFO  [GoldTransform] -- Executing outlier handling, statistical smoothing, and feature isolation.")
    run_log("INFO  [GoldTransform] -- Optimizing model boundaries for F2 score (Prioritizing recall over precision).")
    run_log("INFO  [GoldTransform] -- Verification: Wort yield optimization confirmed at +0.6% yield increase.")
    run_log("INFO  [GoldTransform] Compiling end-to-end portfolio tracking logs from Networth Corp for AB InBev.")
    run_log("INFO  [GoldTransform] Generating finalized resume schemas. Delta Optimize & vacuum complete.")
    run_log("<span style='color:#10B981; font-weight:bold;'>SUCCESS [JobTracker] Job completed successfully! 142,500 rows written to 'db_gold.ashutosh_kumar_resume'</span>", sleep_time=0.8)
    
    # Update DAG Visual 4 -> Final
    c4.markdown("<div class='dag-node-success'>🥇 GOLD LAYER<br><span style='font-size:0.75em; font-weight:normal;'>Completed</span></div>", unsafe_allow_html=True)
    
    st.balloons()
    st.success("🎉 Pipeline complete! Standardized Gold Resume Layer successfully generated and loaded.")
    
    # ── SHOW FINALIZED RESUME DATA ─────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🥇 Gold Layer Delta Table Output: `db_gold.ashutosh_kumar_resume`")
    
    resume_df = pd.DataFrame([
        {"Experience Component": "Data Engineer (BrewDat 3.0)", "Primary Tech": "ADF, Databricks, PySpark, Delta Lake", "Standardized Scope": "Onboarding & standardization of SAP PM Transactions (IW37, IP28, IH24, IW47, ZI203) across global zones.", "Business Value": "Automated manual refresh workflows, reducing turnaround time & eliminating manual error risks."},
        {"Experience Component": "Power BI Developer (ReliabilityGKPI)", "Primary Tech": "Power BI, DAX, MS Excel", "Standardized Scope": "Translating maintenance logs into unified, standardized global reliability KPI dashboards.", "Business Value": "Enabled executive level tracking of plant performance across multiple global operations."},
        {"Experience Component": "Process Optimization (Lauter Tun)", "Primary Tech": "Python, Machine Learning, Signal Processing", "Standardized Scope": "Aggregating 120-min process sensors, smoothing outlier logs, and training models optimized for F2 recall.", "Business Value": "Improved Lauter Tun wort production yield by 0.6%."}
    ])
    
    st.dataframe(resume_df, use_container_width=True)
    
    st.markdown("---")
    
    # ── FINAL RENDERED RESUME ──────────────────────────────────────────
    st.markdown("### 📄 Compiled Executive Data Engineering Resume")
    
    resume_md = """
    # ASHUTOSH KUMAR
    **Data Engineer | Cloud & Pipeline Automation Architect**  
    Bangalore, India | ashutoshkr1005@gmail.com | +91 79923 45808 | [linkedin.com/in/ashutoshkumar42](https://linkedin.com/in/ashutoshkumar42)

    ---

    ### 🛠️ Core Data Engineering Expertise
    *   **Data Infrastructure & Cloud:** Azure Data Factory (ADF), Databricks, Azure Data Lake Storage (ADLS Gen2), PySpark, Delta Lake, Snowflake, Pipeline Design, ETL/ELT.
    *   **Enterprise Systems:** SAP Plant Maintenance (PM) Modules & Tables (IW37, IP28, IH24, IW47, ZI203), BrewDat 3.0 Enterprise Architecture.
    *   **Languages & Analytics:** Python, SQL, PySpark, Statistical Modeling, Outlier Handling, Signal Smoothing & Processing.
    *   **BI & Analytics:** Power BI, DAX, MS Excel, Standardized Reliability KPI Reporting.

    ---

    ### 💼 Professional Experience

    #### **Networth Corp — Bangalore, India** *(Data science & engineering services provider to AB InBev)*
    **Data Engineer — ReliabilityGKPI & Maintenance One** | *Oct 2024 – Present*
    *   **Data Onboarding & Integration:** Architected the onboarding and integration of core SAP Plant Maintenance data (Transactions IW37, IP28, IH24, IW47, and ZI203) into the Azure Delta Lake, establishing a unified global maintenance model.
    *   **Orchestration & ETL:** Designed, built, and optimized automated ELT pipelines in Azure Data Factory to ingest, validate, and error-handle heavy transaction logs across global zones (AFR, EUR, MAZ, APAC, SAZ, NAZ).
    *   **Big Data Transformation:** Leveraged Databricks and PySpark to process and transform raw bronze-level layers into highly conformed, partitioned Silver & Gold Delta Lake tables for downstream consumption.
    *   **Workflow Automation:** Eliminated recurring human error and reduced dashboard refresh turnaround times by automating legacy, manual database-to-reporting refresh pipelines.

    **Power BI Developer — Global Reliability KPI Dashboards** | *Jan 2024 – Oct 2024*
    *   **Metric Standardisation:** Authored complex DAX measures and calculated tables in Power BI to standardize raw SAP maintenance logs into unified reliability KPIs.
    *   **Stakeholder Alignment:** Ran collaborative validation sessions with plant stakeholders and global management to align KPI business rules, and documented the complete end-to-end data lineage for enterprise governance.

    **Data Analyst — Lauter Tun Production Optimization** | *Jan 2022 – Dec 2023*
    *   **Yield Improvement (+0.6%):** Co-led a massive data-driven process optimization of the Lauter Tun brewing pipeline, increasing wort production yields by 0.6% via predictive machine learning models.
    *   **Feature Engineering & Signal Processing:** Cleaned, processed, and smoothed noisy time-series sensor logs from brewing historians, aggregating high-frequency signals into clean 120-minute averaged features to stabilize downstream models.
    *   **Mathematical Model Tuning:** Tuned and optimized models targeting an \\(F_2\\) score to prioritize recall, ensuring the brewing plant successfully caught and mitigated potential low-yield batches.

    ---

    ### 🎓 Education
    *   **B.Tech in Engineering** | University Institute of Technology, University of Burdwan (2016 – 2020) | *Final Project: Trajectory Tracing & Control Logic of a Quadcopter (77.7%)*
    *   **SRC Inter College** (2013 – 2015) | *Physics, Chemistry, Mathematics (71.4%)*
    """
    
    st.markdown(resume_md)
    
    # Download Button
    st.download_button(
        label="📥 Download Standard Plain-Text DE Resume",
        data=resume_md,
        file_name="ashutosh_kumar_data_engineer_resume.txt",
        mime="text/plain"
    )
