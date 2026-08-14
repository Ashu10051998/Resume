import streamlit as st
import time
import pandas as pd

# Set page configuration for a wide, high-impact dashboard layout
st.set_page_config(
    page_title="Ashutosh Kumar | Data Pipeline Resume",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CUSTOM CSS FOR ENTERPRISE PIPELINE ARCHITECTURE (app.2.png Style) ──
st.markdown("""
<style>
    /* Dark Slate Theme Background */
    .stApp {
        background-color: #0F172A;
        color: #E2E8F0;
    }
    
    /* Header styling */
    .pmo-header {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        padding: 24px;
        border-radius: 8px;
        color: white;
        margin-bottom: 25px;
        border-bottom: 4px solid #38BDF8;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.4);
    }
    
    /* Visual workflow styling */
    .workflow-container {
        display: flex;
        flex-direction: row;
        justify-content: space-between;
        gap: 15px;
        margin-top: 20px;
        margin-bottom: 30px;
    }
    
    /* Experience Title (stands above each box) */
    .exp-title-box {
        font-family: 'Helvetica Neue', Arial, sans-serif;
        font-size: 1.05em;
        font-weight: 700;
        color: #38BDF8;
        text-align: center;
        min-height: 55px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 12px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        border-bottom: 2px solid #1E293B;
        padding-bottom: 6px;
    }
    
    /* Dynamic workflow box cards */
    .workflow-card {
        background-color: #1E293B;
        border-radius: 8px;
        padding: 16px;
        border: 2px solid #334155;
        min-height: 480px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        box-shadow: 0 6px 12px rgba(0, 0, 0, 0.25);
        transition: all 0.3s ease;
    }
    
    .workflow-card:hover {
        transform: translateY(-6px);
        border-color: #38BDF8;
        box-shadow: 0 10px 20px rgba(56, 189, 248, 0.15);
    }
    
    /* Box Badges corresponding to app.2.png architecture */
    .layer-badge {
        font-family: 'Courier New', Courier, monospace;
        font-size: 0.78em;
        font-weight: bold;
        padding: 4px 10px;
        border-radius: 4px;
        text-transform: uppercase;
        margin-bottom: 12px;
        display: inline-block;
        text-align: center;
    }
    
    .badge-source { background-color: #3B82F6; color: #FFFFFF; }
    .badge-ingest { background-color: #8B5CF6; color: #FFFFFF; }
    .badge-bronze { background-color: #B45309; color: #FFFFFF; } /* Bronze Tint */
    .badge-silver { background-color: #64748B; color: #FFFFFF; } /* Silver Tint */
    .badge-gold { background-color: #D97706; color: #FFFFFF; } /* Gold Tint */
    
    /* Card content typography */
    .card-headline {
        font-size: 0.9em;
        font-weight: bold;
        color: #F1F5F9;
        margin-bottom: 8px;
    }
    
    .card-bullet {
        font-size: 0.82em;
        line-height: 1.45em;
        color: #CBD5E1;
        margin-bottom: 8px;
        padding-left: 12px;
        text-indent: -12px;
    }
    
    .card-bullet strong {
        color: #38BDF8;
    }

    /* Terminal-style logging panel */
    .terminal-container {
        background-color: #0B0F19;
        border-radius: 6px;
        border: 1px solid #1E293B;
        font-family: 'Courier New', Courier, monospace;
        padding: 15px;
        margin-top: 15px;
    }
    .terminal-header {
        color: #00FFCC;
        border-bottom: 1px solid #1E293B;
        padding-bottom: 8px;
        margin-bottom: 10px;
        font-size: 0.85em;
    }
    .terminal-body {
        color: #E2E8F0;
        font-size: 0.85em;
        height: 180px;
        overflow-y: auto;
        line-height: 1.5em;
    }
    .accent-text {
        color: #00FFCC;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ORCHESTRATION ──────────────────────────────────────────────
st.markdown("""
<div class='pmo-header'>
    <h1 style='margin:0; font-size:2.2em;'>⚡ SYSTEM_WORKFLOW_ORCHESTRATOR v3.0</h1>
    <p style='margin:5px 0 0 0; color:#38BDF8; font-family:monospace;'>
        Candidate: Ashutosh_Kumar // Role: Lead_Data_Engineer // Input: Career_Database_Delta_Lake
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
### 🏗️ Live End-to-End Pipeline & Experience Architecture
Hiring managers shouldn't just read a static resume—they should configure and run the actual data workflow that builds it.
Below is the system-level mapping representing **Ashutosh Kumar's industrial experience** patterned directly after our **Azure & Databricks Medallion Architecture (app.2.png)**. 

Each box represents a core data stage, featuring his **Specific Role / Experience Title** above and his conformed **Professional bullets** inside.
""")

# ── SIDEBAR CONTROLS ──────────────────────────────────────────────────
st.sidebar.markdown("## ⚙️ Cluster & Pipeline Specs")

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
**Target Cluster Specifications:**
*   **Active Driver:** Standard_D4s_v5 (16 GB, 4 Cores)
*   **Worker Scale:** Auto-scaling (2 to 8 nodes)
*   **Database Engine:** Databricks Runtime 14.3 LTS
*   **Metadata Store:** Unity Catalog Enabled
""")

# ── THE INTERACTIVE EXPERIENCE DAG WORKFLOW (app.2.png) ───────────────
st.markdown("### 🗺️ Visual Career Architecture Pipeline (Horizontal Flow)")

# Create 5 columns representing the exact data path in app.2.png
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown("""
    <div class='exp-title-box'>
        Upstream Sourcing
    </div>
    <div class='workflow-card'>
        <span class='layer-badge badge-source'>🌐 HTTP / SAP Source</span>
        <div class='card-headline'>Acrosoft CDC Replication</div>
        <div class='card-bullet'>• Collaborated with upstream <strong>Acrosoft IT Team</strong> to capture and duplicate raw SAP Plant Maintenance ERP logs.</div>
        <div class='card-bullet'>• Governed landing partition rules to manage raw delta files securely inside cloud-based staging zones.</div>
        <div class='card-bullet'>• Ensured <strong>data lineage transparency</strong> from source SAP transactions to downstream cloud targets.</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class='exp-title-box'>
        Ingestion & ELT
    </div>
    <div class='workflow-card'>
        <span class='layer-badge badge-ingest'>⚙️ Data Factory</span>
        <div class='card-headline'>Orchestration & Automation</div>
        <div class='card-bullet'>• Designed and maintained automated <strong>ELT pipelines in Azure Data Factory (ADF)</strong> to ingest massive transactional files.</div>
        <div class='card-bullet'>• Managed robust error-handling, validation boundaries, and pipeline notifications at enterprise scale.</div>
        <div class='card-bullet'>• Eliminated human error risks by automating previously <strong>manual, database-to-reporting refresh workflows</strong>.</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div class='exp-title-box'>
        Bronze Ingestion
    </div>
    <div class='workflow-card'>
        <span class='layer-badge badge-bronze'>🟫 Data Lake Gen2</span>
        <div class='card-headline'>Bronze Raw Appends</div>
        <div class='card-bullet'>• Structured an append-only <strong>Bronze Delta Table</strong> to preserve raw historical records and audit logs.</div>
        <div class='card-bullet'>• Implemented surrogate key generation to execute high-throughput record-level deduplications.</div>
        <div class='card-bullet'>• Verified raw integrity gates on ingested data volumes containing over <strong>142,500 rows</strong> per cycle.</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div class='exp-title-box'>
        Spark Processing
    </div>
    <div class='workflow-card'>
        <span class='layer-badge badge-silver'>🥈 Databricks Silver</span>
        <div class='card-headline'>PySpark Transformations</div>
        <div class='card-bullet'>• Built optimized <strong>PySpark notebooks</strong> on Databricks to clean nested payloads, parse JSON structures, and cast timestamps.</div>
        <div class='card-bullet'>• Standardized complex <strong>SAP Plant Maintenance tables (IW37, IP28, IH24, IW47, and ZI203)</strong>.</div>
        <div class='card-bullet'>• Harmonized data structures across 6 major global operating zones (AFR, EUR, MAZ, APAC, SAZ, NAZ).</div>
        <div class='card-bullet'>• Pre-processed and smoothed high-frequency <strong>industrial sensor signals</strong> from brewing history logs into conformed features.</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown("""
    <div class='exp-title-box'>
        Gold & Reporting
    </div>
    <div class='workflow-card'>
        <span class='layer-badge badge-gold'>🥇 Serving & Power BI</span>
        <div class='card-headline'>KPI Serving & Analytics</div>
        <div class='card-bullet'>• Calculated advanced business KPIs and loaded conformed Gold Delta tables for Power BI consumption.</div>
        <div class='card-bullet'>• Deployed custom ML models on conformed time-series data, driving a <strong>+0.6% wort production yield optimization</strong> for Lauter Tun brewing.</div>
        <div class='card-bullet'>• Authored complex **DAX measures** and calculated tables in Power BI to deliver standardized, global plant performance dashboards.</div>
    </div>
    """, unsafe_allow_html=True)

# ── TRIGGER PIPELINE SIMULATION BUTTON ────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
trigger_col, stats_col = st.columns([1, 2])

with trigger_col:
    st.markdown("### 🚀 Execute ETL Pipeline")
    trigger_btn = st.button("Trigger Job: Build_Gold_Resume()", type="primary", use_container_width=True)

with stats_col:
    st.markdown("### 📊 Active Pipeline Telemetry")
    m_col1, m_col2, m_col3 = st.columns(3)
    with m_col1:
        st.markdown("<div style='background-color:#1E293B; border-left:4px solid #3B82F6; padding:15px; border-radius:4px;'><b>TOTAL RECORDS INGESTED</b><br>142,500</div>", unsafe_allow_html=True)
    with m_col2:
        st.markdown("<div style='background-color:#1E293B; border-left:4px solid #10B981; padding:15px; border-radius:4px;'><b>DAG HEALTH</b><br><span style='color:#10B981;'>100% Operational ✅</span></div>", unsafe_allow_html=True)
    with m_col3:
        st.markdown("<div style='background-color:#1E293B; border-left:4px solid #D97706; padding:15px; border-radius:4px;'><b>COMPRESSION CODEC</b><br>Parquet Snappy</div>", unsafe_allow_html=True)

# ── EXECUTION OUTPUT TERMINAL ─────────────────────────────────────────
if trigger_btn:
    st.markdown("### 💻 Real-Time PySpark Driver Console Log")
    
    # Establish dynamic progress logging
    terminal_placeholder = st.empty()
    logs = []
    
    def run_log(step_text, sleep_time=0.4):
        logs.append(step_text)
        log_html = "".join([f"<div style='margin-bottom: 5px;'>{log}</div>" for log in logs])
        terminal_placeholder.markdown(f"""
        <div class='terminal-container'>
            <div class='terminal-header'>⚡ PySpark @ Driver-Node [{orchestrator.upper().replace(" ", "_")}] // Job_ID: JOB-ASHUTOSH-DE-003</div>
            <div class='terminal-body'>{log_html}</div>
        </div>
        """, unsafe_allow_html=True)
        time.sleep(sleep_time)

    # Trigger simulated execution logs
    run_log("INFO  [JobTracker] Initializing SparkSession with Unity Catalog & Delta Lake support enabled.")
    run_log(f"INFO  [JobTracker] Spark Parameters: Master=yarn-client, Executor Cores=4, Compression={compression_codec.upper()}")
    run_log("INFO  [AcrosoftIngest] Scanning Upstream ADLS Gen2 landing container 'raw-sap-pm/YYYY/MM/DD/'...")
    run_log("INFO  [AcrosoftIngest] Detected newly landed SAP Delta files compiled by Acrosoft IT CDC Service.")
    run_log("INFO  [ADF_Orchestration] Invoking Azure Data Factory event triggers: Raw Landing -> Bronze Table.")
    run_log("INFO  [BronzeWriter] Writing raw appends to delta-parquet: `db_bronze.raw_sap_maintenance`")
    run_log("INFO  [BronzeWriter] Executing row-level deduplications based on surrogate keys.")
    run_log("INFO  [BronzeWriter] Successfully archived 142,500 records into the Bronze Storage layer.")
    run_log("INFO  [DatabricksPySpark] Spinning up distributed executors to execute conformed transformations.")
    run_log("INFO  [DatabricksPySpark] Performing schema enforcement, casting timestamps, and nested payload parsing.")
    run_log("INFO  [DatabricksPySpark] Standardizing SAP PM Transactions: IW37, IP28, IH24, IW47, and ZI203 across global zones.")
    run_log("INFO  [DatabricksPySpark] Writing conformed parquet-backed Silver records: `db_silver.conformed_maintenance_logs`")
    run_log("INFO  [GoldAggregations] Pulling conformed Silver tables to run advanced feature aggregations.")
    run_log("INFO  [GoldAggregations] Running signal processing and statistical outlier smoothing on Lauter Tun historian sensor logs.")
    run_log("INFO  [GoldAggregations] Executing mathematical model evaluations optimized for F2 score (Prioritizing recall).")
    run_log("INFO  [GoldAggregations] Wort yield optimization metrics confirmed: +0.6% production yield increase.")
    run_log("INFO  [DeltaOptimize] Writing finalized conformed views to partition-optimized delta tables.")
    run_log("INFO  [DeltaOptimize] Running OPTIMIZE and VACUUM (retaining 7-day logs) on table `db_gold.ashutosh_kumar_resume`.")
    run_log("<span class='accent-text'>SUCCESS</span> [JobTracker] Ingest_and_Standardize_Resume() completed successfully in 3.8s. 142,500 records committed.", sleep_time=0.6)
    
    st.balloons()
    st.success("🎉 Career Data Pipeline successfully executed! Conformed Delta Gold Layer has been finalized.")
    
    # Render final conformed Gold output
    st.markdown("---")
    st.markdown("### 🥇 Gold Layer Delta Table Output: `db_gold.ashutosh_kumar_resume`")
    
    resume_df = pd.DataFrame([
        {"Experience Component": "Upstream Sourcing (SAP ERP)", "Primary Tech": "Acrosoft CDC, ADLS Gen2", "Standardized Scope": "Collaborative integration with Acrosoft to capture raw ERP change logs and land delta files securely.", "Business Value": "Ensures transparent data lineage from source ERP databases to downstream targets."},
        {"Experience Component": "Ingestion & ELT (Data Factory)", "Primary Tech": "Azure Data Factory (ADF)", "Standardized Scope": "Designed and maintained automated, high-throughput ELT pipelines to ingest raw transactions.", "Business Value": "Eliminated manual workflows, reducing turnaround time & error risks."},
        {"Experience Component": "Bronze Ingestion (Data Lake)", "Primary Tech": "ADLS Gen2, Delta Lake", "Standardized Scope": "Append-only Bronze storage with surrogate key generation and deduplications.", "Business Value": "Preserves complete historical audit logs with 100% schema integrity."},
        {"Experience Component": "Spark Processing (Databricks)", "Primary Tech": "Databricks, PySpark, SQL", "Standardized Scope": "PySpark parsing, nested payload cleaning, and conforment of core SAP PM tables across 6 global zones.", "Business Value": "Transforms dirty, regional silos into conformed, partitioned Silver tables."},
        {"Experience Component": "Gold & Reporting (Serving Layer)", "Primary Tech": "Power BI, DAX, ML, Python", "Standardized Scope": "Gold layer serving, DAX metric standardizations, and predictive ML for brewing yield optimization.", "Business Value": "Drives +0.6% Lauter Tun yield improvement and standardizes global KPI tracking."}
    ])
    
    st.dataframe(resume_df, use_container_width=True)
    
    # ── COMPILED EXECUTIVE RESUME FOR DOWNLOAD ────────────────────────────
    st.markdown("---")
    st.markdown("### 📄 Compiled Executive Data Engineering Resume")
    
    resume_md = """
    # ASHUTOSH KUMAR
    **Lead Data Engineer | Cloud, Pipeline Automation & Lakehouse Architect**  
    Bangalore, India | ashutoshkr1005@gmail.com | +91 79923 45808 | [linkedin.com/in/ashutoshkumar42](https://linkedin.com/in/ashutoshkumar42)

    -----

    ### 🛠️ Technical Competency Spectrum
    *   **Cloud Data Infrastructure:** Azure Data Factory (ADF), Databricks, Azure Data Lake Storage (ADLS Gen2), Delta Lake, PySpark, Snowflake, Pipeline Design, ETL/ELT.
    *   **Enterprise Systems:** SAP Plant Maintenance (PM) Modules & Tables (IW37, IP28, IH24, IW47, ZI203), BrewDat 3.0 Enterprise Architecture.
    *   **Languages & Programming:** Python, SQL, PySpark, Distributed Cluster Computations.
    *   **Advanced Analytics & BI:** Power BI, DAX, MS Excel, Statistical Modeling, Outlier Handling, Signal Smoothing & Processing.

    -----

    ### 💼 Core Professional Footprint

    #### **Networth Corp — Bangalore, India** *(Data science & engineering services provider to AB InBev)*
    **Data Engineer — ReliabilityGKPI & Maintenance One** | *Oct 2024 – Present*
    *   **Data Onboarding & Integration:** Co-designed the architectural onboarding of raw SAP Plant Maintenance data from upstream Acrosoft IT systems into the ADLS landing zones, securing data lineage for transactions IW37, IP28, IH24, IW47, and ZI203.
    *   **Orchestration & ADF Pipelines:** Architected and maintained automated event-triggered ELT pipelines in Azure Data Factory to ingest, parse, and validate transaction records across major global zones (AFR, EUR, MAZ, APAC, SAZ, NAZ).
    *   **PySpark & Delta Transformations:** Developed high-throughput PySpark notebooks inside Databricks to transform raw bronze tables into conformed Silver Delta tables, handling schema evolution and deduplications.
    *   **Automation Execution:** Successfully automated legacy, manual database-to-reporting refresh workloads, reducing turnaround times and ensuring 100% data integrity for plant KPI tracking.

    **Power BI Developer — Global Reliability KPI Dashboards** | *Jan 2024 – Oct 2024*
    *   **Metric Standardization:** Authored complex DAX measures and calculated tables in Power BI to translate conformed Gold maintenance tables into standardized global plant performance KPIs.
    *   **Stakeholder Alignment:** Conducted working sessions with plant managers and higher management to align, validate, and document end-to-end dashboard and data-flow governance rules.

    **Data Analyst — Lauter Tun Production Optimization** | *Jan 2022 – Dec 2023*
    *   **Process Yield Optimization (+0.6%):** Co-led a predictive machine learning optimization initiative on the brewing process, successfully increasing Lauter Tun wort yields by 0.6% in production.
    *   **Feature Engineering & Signal Processing:** Cleaned and smoothed noisy, high-frequency historian sensor logs, aggregating signal spikes into conformed 120-minute averaged intervals to maximize downstream model stability.
    *   **Model Tuning:** Built and optimized model boundaries targeting an \\\\(F_2\\\\) score to prioritize recall, ensuring the plant caught and mitigated potential low-yield brewing batches.

    -----

    ### 🎓 Education Baseline
    *   **B.Tech in Engineering** | University Institute of Technology, University of Burdwan (2016 – 2020) | *Final Project: Trajectory Tracing & Control Logic of a Quadcopter (77.7%)*
    *   **SRC Inter College** (2013 – 2015) | *Physics, Chemistry, Mathematics (71.4%)*
    """
    
    st.markdown(resume_md)
    
    st.download_button(
        label="📥 Download Standard Plain-Text DE Resume",
        data=resume_md,
        file_name="ashutosh_kumar_data_engineer_resume.txt",
        mime="text/plain"
    )
