import streamlit as st
import time
import pandas as pd

# Set page configuration for a professional, data-centric dashboard
st.set_page_config(
    page_title="Ashutosh Kumar | Data Pipeline Resume",
    page_icon="⚡",
    layout="wide"
)

# Custom CSS to render a beautiful, highly polished, dark-themed PMO Data Pipeline dashboard
st.markdown("""
<style>
    .reportview-container {
        background-color: #0E1117;
    }
    .pmo-header {
        background: linear-gradient(135deg, #1F3A60 0%, #0F1E36 100%);
        padding: 25px;
        border-radius: 8px;
        color: white;
        margin-bottom: 25px;
        border-bottom: 4px solid #00D2FF;
    }
    .flowchart-container {
        display: flex;
        align-items: stretch;
        justify-content: space-between;
        gap: 10px;
        overflow-x: auto;
        padding: 15px 5px;
        background-color: #0F172A;
        border-radius: 8px;
        border: 1px solid #1E293B;
        margin-bottom: 30px;
    }
    .flow-node {
        flex: 1;
        min-width: 170px;
        display: flex;
        flex-direction: column;
    }
    .node-header {
        text-align: center;
        font-family: 'Courier New', Courier, monospace;
        font-weight: bold;
        color: #38BDF8;
        font-size: 0.8em;
        margin-bottom: 8px;
        min-height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .node-card {
        background-color: #1E293B;
        border: 2px solid #475569;
        border-radius: 8px;
        padding: 12px;
        flex-grow: 1;
        color: #94A3B8;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .node-card-active {
        background-color: #1E293B;
        border: 2px solid #00FFCC;
        color: #00FFCC;
        box-shadow: 0 0 15px rgba(0, 255, 204, 0.3);
    }
    .node-card-success {
        background-color: #0F172A;
        border: 2px solid #10B981;
        color: #E2E8F0;
    }
    .node-icon {
        font-size: 1.4em;
        margin-bottom: 5px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .node-title {
        font-weight: bold;
        font-size: 0.85em;
        margin-bottom: 8px;
        color: white;
    }
    .node-card-success .node-title {
        color: #10B981;
    }
    .node-body {
        font-size: 0.75em;
        line-height: 1.3em;
    }
    .node-arrow {
        display: flex;
        align-items: center;
        justify-content: center;
        color: #475569;
        font-size: 1.2em;
        padding: 0 2px;
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
    <h1 style='margin:0; font-size:2em;'>⚡ ENTERPRISE RESUME PIPELINE ORCHESTRATOR</h1>
    <p style='margin:5px 0 0 0; color:#38BDF8; font-family:monospace;'>
        Target: Ashutosh_Kumar_Resume // Environment: Medallion_Architecture // Status: Active_Listener
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
### 🗺️ Visual Career Data Flow (Based on app2.py)
 hHiring managers can run this pipeline to see how raw experience data is ingested, standardises regional transactions, and serves as an optimized Gold Layer.
""")

# Setup Sidebar Configurations
st.sidebar.markdown("## ⚙️ Infrastructure & Compute Config")

orchestrator = st.sidebar.selectbox(
    "Orchestrator Engine",
    ["Azure Data Factory (ADF)", "Apache Airflow / DAG", "Databricks Workflows"]
)

compute_engine = st.sidebar.selectbox(
    "Compute Engine",
    ["Apache Spark / PySpark", "Databricks Delta Live Tables (DLT)", "Azure Synapse Spark"]
)

compression_codec = st.sidebar.selectbox(
    "Compression Codec",
    ["Snappy (Parquet Default)", "GZIP", "ZSTD"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("""
**Cluster Hardware:**
*   **Driver:** Standard_D4s_v5 (16GB, 4 Cores)
*   **Workers:** Auto-scaling (2 to 8 nodes)
*   **Storage Account:** ADLS Gen2 ADLS Hierarchy
""")

# Action Button to Trigger the ETL Job
trigger_pipeline = st.button("🚀 Run ETL Pipeline: Build_Gold_Resume()", type="primary", use_container_width=True)

# Define columns representing the 7 workflow steps from app2.py
# Box 1 -> Arrow -> Box 2 -> Arrow -> Box 3 -> Arrow -> Box 4 -> Arrow -> Box 5 -> Arrow -> Box 6 -> Arrow -> Box 7
# To handle 7 boxes + 6 arrows = 13 columns, we can set exact ratios
cols = st.columns([12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12, 1, 12])

# Placeholders for our cards so they can update dynamically
n1 = cols[0].empty()
a1 = cols[1].empty()
n2 = cols[2].empty()
a2 = cols[3].empty()
n3 = cols[4].empty()
a3 = cols[5].empty()
n4 = cols[6].empty()
a4 = cols[7].empty()
n5 = cols[8].empty()
a5 = cols[9].empty()
n6 = cols[10].empty()
a6 = cols[11].empty()
n7 = cols[12].empty()

# Static / Initial State Rendering Function
def render_nodes(state="initial"):
    # Initial: 1 active, others inactive. Active: running. Success: all success.
    card_styles = {
        "n1": "node-card-success" if state == "success" else ("node-card-active" if state == "n1" else "node-card-inactive"),
        "n2": "node-card-success" if state == "success" or state in ["n3", "n4", "n5", "n6", "n7"] else ("node-card-active" if state == "n2" else "node-card-inactive"),
        "n3": "node-card-success" if state == "success" or state in ["n4", "n5", "n6", "n7"] else ("node-card-active" if state == "n3" else "node-card-inactive"),
        "n4": "node-card-success" if state == "success" or state in ["n5", "n6", "n7"] else ("node-card-active" if state == "n4" else "node-card-inactive"),
        "n5": "node-card-success" if state == "success" or state in ["n6", "n7"] else ("node-card-active" if state == "n5" else "node-card-inactive"),
        "n6": "node-card-success" if state == "success" or state == "n7" else ("node-card-active" if state == "n6" else "node-card-inactive"),
        "n7": "node-card-success" if state == "success" else ("node-card-active" if state == "n7" else "node-card-inactive"),
    }
    
    arrow_color = "#10B981" if state == "success" else "#475569"

    # Box 1: Upstream Sourcing
    n1.markdown(f"""
    <div class="flow-node">
        <div class="node-header">UPSTREAM SOURCING</div>
        <div class="node-card {card_styles['n1']}">
            <div class="node-icon">🌐 <span style="font-size:0.6em; color:#38BDF8;">SOURCE</span></div>
            <div class="node-title">HTTP / SAP Source</div>
            <div class="node-body">
                • Partnered with the <b>Acrosoft team</b> to ingest and copy raw SAP Plant Maintenance logs to raw landing zones.<br>
                • Defined clear integration boundaries for incoming transaction feeds.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a1.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 2: Ingestion & ELT
    n2.markdown(f"""
    <div class="flow-node">
        <div class="node-header">INGESTION & ELT</div>
        <div class="node-card {card_styles['n2']}">
            <div class="node-icon">⚙️ <span style="font-size:0.6em; color:#38BDF8;">ADF</span></div>
            <div class="node-title">Data Ingestion</div>
            <div class="node-body">
                • Designed robust, automated ingestion pipelines in <b>Azure Data Factory (ADF)</b> for transactional logs.<br>
                • Automated manual dashboard refresh workflows to eliminate human errors.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a2.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 3: Raw Data Store (Bronze)
    n3.markdown(f"""
    <div class="flow-node">
        <div class="node-header">BRONZE STORAGE</div>
        <div class="node-card {card_styles['n3']}">
            <div class="node-icon">🟫 <span style="font-size:0.6em; color:#CD7F32;">🥉 BRONZE</span></div>
            <div class="node-title">Raw Data Store</div>
            <div class="node-body">
                • Engineered partition structures in <b>Azure Data Lake (ADLS Gen2)</b>.<br>
                • Implemented append-only Bronze tables with rigorous surrogate key deduplication.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a3.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 4: Transformation (Databricks)
    n4.markdown(f"""
    <div class="flow-node">
        <div class="node-header">SPARK PROCESSING</div>
        <div class="node-card {card_styles['n4']}">
            <div class="node-icon">🧱 <span style="font-size:0.6em; color:#38BDF8;">SPARK</span></div>
            <div class="node-title">Transformation</div>
            <div class="node-body">
                • Developed scalable, parallelized cleaning and parsing notebooks in <b>Databricks and PySpark</b>.<br>
                • Standardized transactional data arrays for downstream consumption.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a4.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 5: Transformed Data (Silver)
    n5.markdown(f"""
    <div class="flow-node">
        <div class="node-header">SILVER STANDARDS</div>
        <div class="node-card {card_styles['n5']}">
            <div class="node-icon">🥈 <span style="font-size:0.6em; color:#C0C0C0;">🥈 SILVER</span></div>
            <div class="node-title">Transformed Data</div>
            <div class="node-body">
                • Unified legacy SAP PM tables (<b>IW37, IP28, IH24, IW47, ZI203</b>) across 6 global zones.<br>
                • Performed advanced outlier handling and telemetry signal smoothing.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a5.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 6: Serving (Synapse - Gold)
    n6.markdown(f"""
    <div class="flow-node">
        <div class="node-header">GOLD SERVING</div>
        <div class="node-card {card_styles['n6']}">
            <div class="node-icon">🏆 <span style="font-size:0.6em; color:#FFD700;">🥇 GOLD</span></div>
            <div class="node-title">Serving</div>
            <div class="node-body">
                • Optimized Gold-tier Delta tables for query performance.<br>
                • Deployed predictive models delivering a <b>+0.6% wort production yield increase</b> in production.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    a6.markdown(f"<div class='node-arrow' style='height:100%; color:{arrow_color};'>➔</div>", unsafe_allow_html=True)

    # Box 7: Reporting (Power BI)
    n7.markdown(f"""
    <div class="flow-node">
        <div class="node-header">BI REPORTING</div>
        <div class="node-card {card_styles['n7']}">
            <div class="node-icon">📊 <span style="font-size:0.6em; color:#38BDF8;">REPORTS</span></div>
            <div class="node-title">Reporting</div>
            <div class="node-body">
                • Authored advanced <b>DAX formulas</b> and measures to compile unified global Reliability KPIs.<br>
                • Delivered executive-ready dashboards for plant-level stakeholder analytics.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Run initial render
render_nodes("initial")

# Handle simulation run
if trigger_pipeline:
    st.markdown("### 🖥️ Real-Time PySpark Console Log")
    
    # Set up log space
    terminal_placeholder = st.empty()
    logs = []
    
    def run_log(step_text, node_focus, sleep_time=0.5):
        logs.append(step_text)
        log_html = "".join([f"<div style='margin-bottom: 5px;'>{log}</div>" for log in logs])
        terminal_placeholder.markdown(f"""
        <div class='terminal-container'>
            <div class='terminal-header'>⚡ PySpark @ Driver-Node [{orchestrator.upper().replace(" ", "_")}] // Job: Ingest_And_Standardize_Resume</div>
            <div class='terminal-body'>{log_html}</div>
        </div>
        """, unsafe_allow_html=True)
        render_nodes(node_focus)
        time.sleep(sleep_time)

    # 1. Source (HTTP)
    run_log("INFO  [JobTracker] Initializing SparkSession with Delta Lake support enabled.", "n1")
    run_log("INFO  [JobTracker] Using Spark Executor Cores: " + str(st.sidebar.slider("Cores Status", 1, 8, 4)) + f" // Compression Codec: {compression_codec}.", "n1")
    run_log("INFO  [AcrosoftIT] Polling raw SAP source tables via CDC listener...", "n1")
    run_log("INFO  [AcrosoftIT] Replicating legacy PM database transaction logs.", "n1")
    run_log("SUCCESS [AcrosoftIT] Successfully copied conformed CDC logs to target ADLS Gen2 landing container.", "n1")

    # 2. Ingestion (ADF)
    run_log("INFO  [ADF_Orchestrator] ADF Event trigger detected new landed files.", "n2")
    run_log("INFO  [ADF_Orchestrator] Running pipeline 'SAP_PM_Ingestion_To_Bronze'...", "n2")
    run_log("INFO  [ADF_Orchestrator] Executing automated trigger schema validation checks...", "n2")
    run_log("SUCCESS [ADF_Orchestrator] Data ingestion complete. Initiating Spark cluster worker nodes.", "n2")

    # 3. Bronze Ingestion
    run_log("INFO  [BronzeWriter] Loading raw CSV and JSON logs into Databricks file system.", "n3")
    run_log("INFO  [BronzeWriter] Enforcing append-only Delta structures for target table: `db_bronze.raw_career_history`", "n3")
    run_log("INFO  [BronzeWriter] Running surrogate key generation and deduplication check...", "n3")
    run_log("SUCCESS [BronzeWriter] Deduplicated 1,420 redundant keys. Written raw rows to Bronze Delta Lake.", "n3")

    # 4. Spark Processing (Databricks)
    run_log("INFO  [DatabricksSpark] Instantiating distributed Databricks executors for PySpark notebook.", "n4")
    run_log("INFO  [DatabricksSpark] Loading `db_bronze.raw_career_history` into memory-optimized DataFrame.", "n4")
    run_log("INFO  [DatabricksSpark] Transforming nested JSON strings and parsing structured arrays.", "n4")

    # 5. Silver Transformation
    run_log("INFO  [SilverTransform] Re-indexing and auditing structural schemas for transaction data...", "n5")
    run_log("INFO  [SilverTransform] Mapping legacy transactions to SAP PM schemas (IW37, IP28, IH24, IW47, ZI203)...", "n5")
    run_log("INFO  [SilverTransform] Standardizing global zonal layers across global regions: AFR, EUR, MAZ, APAC, SAZ, NAZ.", "n5")
    run_log("INFO  [SilverTransform] Executing time-series signal processing: smoothing noisy sensor logs and handling outliers.", "n5")
    run_log("SUCCESS [SilverTransform] Schema conformed. Written to table: `db_silver.conformed_career_history`", "n5")

    # 6. Gold Serving (Synapse / Gold Delta)
    run_log("INFO  [GoldCuration] Synthesizing aggregated features and analytics KPIs.", "n6")
    run_log("INFO  [GoldCuration] Dedeploying Lauter Tun machine learning models optimized for F2 recall.", "n6")
    run_log("INFO  [GoldCuration] Verification check: Production model confirmed +0.6% wort yield increase.", "n6")
    run_log("INFO  [GoldCuration] Optimizing Gold Delta tables. Running Delta OPTIMIZE and VACUUM...", "n6")
    run_log("SUCCESS [GoldCuration] Conformed Gold tables successfully written.", "n6")

    # 7. BI Reporting (Power BI)
    run_log("INFO  [PowerBI_Service] Refreshing downstream DirectQuery/Import semantic models.", "n7")
    run_log("INFO  [PowerBI_Service] Refreshing custom DAX metrics: Global Reliability KPIs.", "n7")
    run_log("SUCCESS [JobTracker] Job completed successfully! 142,500 rows written to serving layer.", "success")

    st.balloons()
    st.success("🎉 Career Data Pipeline successfully executed! Scroll down to view the conformed Gold Resume table and download.")

    # ── DELTA TABLE PREVIEW ───────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🥇 Gold Delta Table Partition Preview: `db_gold.ashutosh_kumar_resume`")
    
    resume_df = pd.DataFrame([
        {
            "Experience Stage": "1. Upstream Sourcing",
            "Core Technologies": "HTTP / SAP ERP Database",
            "Standardized Scope / Deliverable": "Co-managed raw SAP PM CDC replication with Acrosoft team.",
            "Business Value / Outcome": "Created solid data demarcation lines for team handoffs."
        },
        {
            "Experience Stage": "2. Ingestion & ELT",
            "Core Technologies": "Azure Data Factory (ADF)",
            "Standardized Scope / Deliverable": "Automated pipeline trigger orchestration and database refreshes.",
            "Business Value / Outcome": "Eliminated human error risks & reduced dashboard refresh lag."
        },
        {
            "Experience Stage": "3. Bronze Ingestion",
            "Core Technologies": "ADLS Gen2 Storage, Delta",
            "Standardized Scope / Deliverable": "Structured append-only Bronze storage & handled surrogate keys.",
            "Business Value / Outcome": "Cleaned up 1,420 duplicates to maintain raw data integrity."
        },
        {
            "Experience Stage": "4. Spark Processing",
            "Core Technologies": "Databricks, PySpark, SQL",
            "Standardized Scope / Deliverable": "Coded memory-optimized JSON parsers and arrays processing.",
            "Business Value / Outcome": "Prepared clean conformed logs for modeling."
        },
        {
            "Experience Stage": "5. Silver Transformation",
            "Core Technologies": "PySpark, Signal Processing",
            "Standardized Scope / Deliverable": "Standardized transactions IW37, IP28, IH24, IW47, ZI203 across 6 zones.",
            "Business Value / Outcome": "Smoothed noisy sensor signals and aligned global tables."
        },
        {
            "Experience Stage": "6. Gold Serving",
            "Core Technologies": "Databricks Delta Lake",
            "Standardized Scope / Deliverable": "Optimized gold partitions & deployed Lauter Tun ML models (F2 optimized).",
            "Business Value / Outcome": "Drove a confirmed +0.6% increase in production wort yield."
        },
        {
            "Experience Stage": "7. BI Reporting",
            "Core Technologies": "Power BI, DAX, MS Excel",
            "Standardized Scope / Deliverable": "Authored advanced global reliability KPI metrics.",
            "Business Value / Outcome": "Enabled executive-level tracking of plants performance."
        }
    ])
    
    st.dataframe(resume_df, use_container_width=True)

    # ── FINAL RENDERED RESUME ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📄 Compiled Executive Data Engineering Resume")
    
    resume_md = """
    # ASHUTOSH KUMAR
    **Data Engineer | Cloud & Pipeline Automation Architect**  
    Bangalore, India | ashutoshkr1005@gmail.com | +91 79923 45808 | [linkedin.com/in/ashutoshkumar42](https://linkedin.com/in/ashutoshkumar42)

    ---

    ### 🛠️ Core Data Engineering Expertise
    *   **Data Infrastructure & Cloud:** Azure Data Factory (ADF), Databricks, Azure Data Lake Storage (ADLS Gen2), PySpark, Delta Lake, Pipeline Design, ETL/ELT.
    *   **Enterprise Systems:** SAP Plant Maintenance (PM) Modules & Tables (IW37, IP28, IH24, IW47, ZI203), BrewDat 3.0 Enterprise Architecture.
    *   **Languages & Analytics:** Python, SQL, PySpark, Statistical Modeling, Outlier Handling, Signal Smoothing & Processing.
    *   **BI & Analytics:** Power BI, DAX, MS Excel, Standardized Reliability KPI Reporting.

    ---

    ### 💼 Professional Experience (Medallion Flow Architecture)

    #### **Networth Corp — Bangalore, India** *(Data science & engineering services provider to AB InBev)*
    **Data Engineer — ReliabilityGKPI & Maintenance One** | *Oct 2024 – Present*
    *   **Upstream Data Ingestion:** Collaborated directly with the Acrosoft IT team to ingest raw SAP PM transactional data directly into hierarchical ADLS Gen2 landing zones.
    *   **Pipeline Automation (ADF):** Designed, built, and optimized automated ELT pipelines in Azure Data Factory (ADF) to ingest and validate transaction logs across global zones (AFR, EUR, MAZ, APAC, SAZ, NAZ), automating legacy manual database-to-reporting refreshes.
    *   **Databricks PySpark Processing:** Developed highly scalable PySpark transformations in Databricks to clean nested JSON inputs, conform data schemas, and standardize core transactions (**IW37, IP28, IH24, IW47, and ZI203**).
    *   **Delta Lake Serving Layer:** Managed and optimized Medallion Bronze, Silver, and Gold Delta Lake tables, incorporating partition schemes and vacuum properties to support performant downstream Business Intelligence.

    **Power BI Developer — Global Reliability KPI Dashboards** | *Jan 2024 – Oct 2024*
    *   **Global Standardization:** Authored complex Power BI DAX formulas to standardise raw transaction records into standardized global Reliability KPIs.
    *   **Governance & Lineage:** Documented full end-to-end data lineage across the pipelines and led working sessions with global stakeholders to lock down business KPI calculations.

    **Data Analyst — Lauter Tun Production Optimization** | *Jan 2022 – Dec 2023*
    *   **Yield Improvement (+0.6%):** Co-led a massive data-driven process optimization of the Lauter Tun brewing pipeline, increasing wort production yields by 0.6% via predictive machine learning models.
    *   **Feature Engineering & Signal Processing:** Cleaned, processed, and smoothed noisy time-series sensor logs from brewing historians, aggregating high-frequency signals into clean 120-minute averaged features.
    *   **Model Tuning:** Optimized predictive models targeting an $F_2$ score to prioritize recall over precision, minimizing missed low-yield events.

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
else:
    st.info("💡 Click the button above to execute the PySpark Ingestion Pipeline and generate the interactive resume.")
