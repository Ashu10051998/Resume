import streamlit as st
import time
import pandas as pd

# Set page configuration
st.set_page_config(
    page_title="Ashutosh Kumar | ETL Pipeline Resume",
    page_icon="⚙️",
    layout="centered"
)

# Custom CSS for the terminal and executive look
st.markdown("""
<style>
    .terminal-header {
        background-color: #1E1E1E;
        color: #00FF00;
        font-family: 'Courier New', Courier, monospace;
        padding: 10px;
        border-radius: 5px 5px 0 0;
        border-bottom: 2px solid #333;
    }
    .terminal-body {
        background-color: #121212;
        color: #F8F8F2;
        font-family: 'Courier New', Courier, monospace;
        padding: 15px;
        border-radius: 0 0 5px 5px;
        margin-bottom: 20px;
        font-size: 0.9em;
        line-height: 1.4em;
    }
    .accent-text {
        color: #00FF00;
        font-weight: bold;
    }
    .gold-box {
        border-left: 5px solid #2F5496;
        padding-left: 15px;
        background-color: #F2F4F8;
        border-radius: 4px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# App Title & Subtitle
st.title("⚙️ The Resume-as-a-Pipeline")
st.subheader("Data Engineering Interactive Recruiter Portal")
st.markdown("""
Welcome! Instead of reading a static document, you can execute **Ashutosh Kumar's Data Onboarding & Transformation Pipeline** 
to process his professional experience and synthesize his final executive resume.
""")

# Setup Sidebar Context
st.sidebar.title("Pipeline Controls")
target_role = st.sidebar.selectbox("Select Target Analytics Schema", ["Data Engineer (DE) Core", "Data Scientist (DS) Core"])
spark_executor_cores = st.sidebar.slider("Spark Executor Cores", 1, 8, 4)
compression_codec = st.sidebar.selectbox("Delta Lake Compression Codec", ["snappy", "gzip", "none"])

st.write("---")

# Visual Representation of the Pipeline Stages
st.markdown("### Interactive Medallion Architecture Flow")
col1, col2, col3 = st.columns(3)
with col1:
    st.info("**1. Raw Ingestion (Bronze)**\n\nIngest raw academic logs and industrial history tables.")
with col2:
    st.warning("**2. Transform & Clean (Silver)**\n\nEnforce schemas, handle nulls, and standardize SAP PM transaction maps.")
with col3:
    st.success("**3. Business Ready (Gold)**\n\nAggregate metrics and compile the finalized target resume.")

# Action Button to Trigger the ETL Job
trigger_pipeline = st.button("🚀 Run ETL Pipeline: Ingest_and_Standardize_Resume()", type="primary")

if trigger_pipeline:
    st.write("### 🖥️ Spark Driver Console & Execution Logs")
    
    # Simulating Spark execution console
    with st.empty():
        terminal_placeholder = st.container()
        
        # Simulated Real-time Logging Steps
        logs = []
        
        def update_console(step_text, progress_val):
            logs.append(step_text)
            log_html = "".join([f"<div>{log}</div>" for log in logs])
            terminal_placeholder.markdown(f"""
            <div class='terminal-header'>⚡ PySpark @ Master: Cluster_ID [NC-ABI-SPARK] - Active Job</div>
            <div class='terminal-body'>
                {log_html}
            </div>
            """, unsafe_allow_html=True)
            time.sleep(0.6)

        update_console("INFO  [SparkSession] Initializing Spark Session on cluster...", 10)
        update_console(f"INFO  [SparkSession] Allocating {spark_executor_cores} Executor Cores for job processing.", 20)
        update_console("INFO  [BronzeIngest] Ingesting 'Ashutosh_Raw_Experience_v3.csv' from DBFS storage...", 30)
        update_console("INFO  [BronzeIngest] Ingesting 'SAP_Plant_Maintenance_Logs.parquet'...", 40)
        update_console("<span class='accent-text'>SUCCESS</span> [BronzeIngest] Read completed: 14 rows loaded into Bronze Delta table.", 50)
        
        update_console("INFO  [SilverTransform] Applying schema boundaries and casting datatypes...", 55)
        update_console("INFO  [SilverTransform] Filtering and partitioning by target schema: **" + target_role + "**", 60)
        update_console("WARN  [SilverTransform] Identified null values in 'Industrial Gap Log' -> Imputing using forward-fill strategy...", 65)
        update_console("INFO  [SilverTransform] Mapping SAP PM modules (standardizing Transactions IW37, IP28, IH24, IW47, ZI203)...", 75)
        update_console("<span class='accent-text'>SUCCESS</span> [SilverTransform] Quality gates passed. 100% of data standardized.", 80)
        
        update_console("INFO  [GoldCuration] Synthesizing aggregated features (e.g., Wort Yield +0.6% Optimization)...", 85)
        update_console(f"INFO  [GoldCuration] Writing optimized data to Gold Delta Lake Layer with '{compression_codec}' compression...", 90)
        update_console("<span class='accent-text'>SUCCESS</span> [GoldCuration] Delta write complete. Target schema compiled successfully.", 100)

    st.balloons()
    st.success("🎉 ETL Pipeline Completed Successfully! Scroll down to view and download the compiled Gold Resume Layer.")

    # Render the Resume
    st.write("---")
    st.markdown("## 📄 COPILED GOLD DELTA TABLE: RESUME VIEW")
    
    if target_role == "Data Engineer (DE) Core":
        st.markdown("""
        ### **ASHUTOSH KUMAR**
        **Data Engineer | Cloud & Infrastructure Specialist**  
        📍 Bangalore, India | 📧 ashutoshkr1005@gmail.com | 📞 +91 79923 45808 | 🔗 [LinkedIn](https://linkedin.com/in/ashutoshkumar42)

        ---

        #### **💼 PROFESSIONAL SUMMARY**
        Enterprise Data Engineer with extensive hands-on experience designing, automating, and maintaining high-throughput cloud-scale data infrastructure in the CPG/industrial analytics space. Expert in building Azure Data Factory (ADF) orchestrated Databricks Medallion pipelines, standardizing complex SAP Plant Maintenance transaction data, and converting unaligned, high-frequency industrial data streams into production-ready analytical assets.

        ---

        #### **🛠️ THE TECHNICAL CORE (GOLD SCHEMAS)**
        * **Data Infrastructure & Cloud**: Azure Data Factory, Azure Data Lake, Databricks, PySpark, Databricks Delta Lake, ETL/ELT Pipeline Design.
        * **Programming**: Python, SQL, PySpark, Spark SQL.
        * **Enterprise Systems**: SAP PM Modules (IW37, IP28, IH24, IW47, ZI203), BrewDat 3.0 Architecture.
        * **BI & Analytics**: Power BI, Custom DAX, MS Excel.

        ---

        #### **📈 PROFESSIONAL EXPERIENCE**

        ##### **Networth Corp — Bangalore, India**  
        *Data science & engineering services provider to AB InBev*  
        **Lead Data Engineer | ReliabilityGKPI & Maintenance One** *(Oct 2024 – Present)*
        * **Medallion Pipeline Engineering**: Architected and managed scalable ETL pipelines in **Azure Data Factory** and **Databricks** using PySpark to ingest and process raw SAP plant maintenance transaction logs.
        * **SAP Data Standardization**: Successfully onboarded and structured core SAP plant maintenance logs (**IW37, IP28, IH24, IW47, and ZI203**) into unified Delta Lake schemas, enabling global analytics reporting.
        * **Process Automation**: Re-engineered manual weekly data collation and staging processes into fully automated orchestrated schedules, **eliminating recurring manual error rates** and reducing turnaround times.
        * **Downstream Integration**: Partnered with Analytics teams to structure curated Gold-tier tables optimized for high-performance Power BI dashboard consumption.

        ##### **Metlife — Power BI Developer**  
        **Global Reliability KPI Dashboards** *(Jan 2024 – Oct 2024)*
        * Constructed global enterprise reliability dashboards consumed by global operations leaders and executives to monitor asset performance across plants.
        * Authored complex **DAX measures and calculated tables** to transform raw transactional database records into standardized global reliability KPIs.
        * Standardized and documented long-term data-flow logic to ensure cross-team transparency and maintainability.

        ##### **Lauter Tun Production Optimization**  
        **Data Analyst & Pipeline Developer** *(Jan 2022 – Dec 2023)*
        * Built data collection pipelines to ingest raw industrial historian time-series and batch-wise process sensor data.
        * Performed advanced data cleaning, signal smoothing, and **outlier handling** on noisy raw sensor feeds.
        * Structured high-frequency raw telemetry records into 120-minute moving-average intervals to align telemetry with brewing process physics and stabilize predictive model features.

        ---

        #### **🎓 EDUCATION & BACKGROUND**
        * **University Institute of Technology, University of Burdwan** (2016 – 2020)  
          *Bachelor of Technology (B.Tech.) | Final Project: Quadcopter Trajectory Tracing (77.7%)*
        """)
    else:
        st.markdown("""
        ### **ASHUTOSH KUMAR**
        **Data Scientist | Machine Learning Specialist**  
        📍 Bangalore, India | 📧 ashutoshkr1005@gmail.com | 📞 +91 79923 45808 | 🔗 [LinkedIn](https://linkedin.com/in/ashutoshkumar42)

        ---

        #### **💼 PROFESSIONAL SUMMARY**
        Problem-solving Data Scientist with deep experience transforming complex, high-frequency industrial and transactional data into actionable business yield. Demonstrated history of deploying predictive machine learning models in production workflows, optimizing physical manufacturing parameters, and translating unstructured sensor streams into high-impact statistical features. 

        ---

        #### **🛠️ THE TECHNICAL CORE (ML SCHEMAS)**
        * **Data Science & ML**: Machine Learning, Statistical Modeling, Predictive Analytics, Feature Engineering, Signal Processing.
        * **Wrangling & Analysis**: Python, PySpark, SQL, Outlier Handling, Noise Filtering.
        * **Orchestration & Scale**: Azure Data Factory, Databricks, Spark ML.
        * **BI & Stakeholder Delivery**: Power BI, Custom DAX, Interactive Visualization.

        ---

        #### **📈 PROFESSIONAL EXPERIENCE**

        ##### **Lauter Tun Production Optimization**  
        **Lead Data Scientist** *(Jan 2022 – Dec 2023)*
        * **Yield Optimization**: Led a high-impact predictive analytics initiative on the physical brewing process, delivering an **on-target 0.6% wort production yield increase** via predictive process constraints.
        * **Signal & Feature Engineering**: Developed custom PySpark pre-processing logic to filter noisy, high-frequency historian sensor telemetry, executing outlier handling and smoothing.
        * **Process Window Alignment**: Implemented a 120-minute rolling aggregation logic to translate real-time sensor metrics into robust batch-level statistical features.
        * **Model Optimization**: Custom-tuned model boundaries to **maximize the $F_2$ score**, strategically biasing performance to prioritize model recall over precision, preventing costly missed low-yield events.
        * **Stakeholder Deployment**: Partnered directly with physical operations teams to integrate models into the active manufacturing plant's daily control workflow.

        ##### **Networth Corp — Bangalore, India**  
        *Data science & engineering services provider to AB InBev*  
        **Analytics & Pipeline Developer | ReliabilityGKPI** *(Oct 2024 – Present)*
        * Programmed robust data integration logic in **Databricks and PySpark** to clean and prepare core SAP transaction tables.
        * Cleaned and structured transactional data arrays to support advanced reliability model modeling and tracking.
        * Replaced legacy manual reporting templates with automated pipelines to provide fresh data models to analysts.

        ##### **Metlife — Power BI Developer**  
        **Global Reliability KPI Dashboards** *(Jan 2024 – Oct 2024)*
        * Engineered analytical data models to power executive dashboard tracking of equipment reliability.
        * Programmed custom **DAX mathematical tables** to standardize global telemetry metrics.

        ---

        #### **🎓 EDUCATION & BACKGROUND**
        * **University Institute of Technology, University of Burdwan** (2016 – 2020)  
          *Bachelor of Technology (B.Tech.) | Final Project: Quadcopter Trajectory Tracing (77.7%)*
        """)

    # Provide download option
    st.download_button(
        label="📥 Download This Compiled Gold Resume Layer (.txt)",
        data="ASHUTOSH KUMAR RESUME\nTarget Role: " + target_role + "\nAll data compiled from Gold Delta Lake Table.",
        file_name=f"ashutosh_kumar_{target_role.lower().replace(' ', '_')}.txt",
        mime="text/plain"
    )
else:
    st.info("💡 Click the button above to execute the PySpark Ingestion Pipeline and generate the interactive resume.")
