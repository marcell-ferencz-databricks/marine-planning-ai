# Import necessary libraries
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from datetime import datetime
from PIL import Image

# Configuration for Databricks connection
cfg = Config(host="https://adb-984752964297111.11.azuredatabricks.net/")  # Set the DATABRICKS_HOST environment variable when running locally

try:
    # if running on Databricks, fetch user email
    w = WorkspaceClient()
    CURRENT_USER = w.current_user.me().emails[0].value
except:
    # if running locally, use a default user
    CURRENT_USER = "local_test_user"

SQL_HTTP_PATH = "/sql/1.0/warehouses/148ccb90800933a1"

# Table names for storing data
DOC_REFERENCE_TABLE = "marcell.marine_planning.parsed_pdfs"
PARAGRAPH_TARGET_TABLE = "marcell.marine_planning.app_paragraph_stage"
DOCUMENT_TARGET_TABLE = "marcell.marine_planning.app_document_stage"
PROJECT_TARGET_TABLE = "marcell.marine_planning.app_project_stage"
SUMMARY_FEEDBACK_TABLE = "marcell.marine_planning.app_feedback"

# AI models used for processing
MODEL_1 = "databricks-meta-llama-3-1-8b-instruct"
MODEL_2 = "databricks-meta-llama-3-3-70b-instruct"
MODEL_3 = "databricks-meta-llama-3-3-70b-instruct"

# Default prompts for AI models
prompt_1_default = """
You are an environmental policy expert. You are given a paragraph or excerpt from an environmental planning correspondence document. If the text pertains to any impacts on marine life, extract the text in complete sentences. If the text does not discuss topics potentially related to marine impact, output <NO IMPACT IDENTIFIED>. If the excerpt on the page looks like a table of contents, a bibliographical reference, or incomplete, ignore the excerpt and output <NO IMPACT IDENTIFIED>. Do not output anything else, apart from an exact extract of the text, or <NO IMPACT IDENTIFIED>. Here's the text: \n\n
"""

prompt_2_default = """
You are an environment policy expert. You are given excerpts from a decision correspondence, where each excerpt pertains to potential or confirmed impact on marine life. Review the document and identify all instances where the Secretary of State (SoS) concluded that adverse effects on integrity (AEoI) cannot be ruled out beyond reasonable scientific doubt. For each instance, extract the specific impact, the quantitative impacts, affected species, locations and any compensation measures. Present these findings as a bulleted list of coherent and succinct sentences with page number references where this information appears in the document. Here is the text: \n\n
"""

prompt_3_default = """
You are an environmental policy expert. You are given summary lists of issues pertaining to impact of infrastructure development on marine life, which have been identified in separate documents. Create a summary list of coherent sentences from these issues, merging any similar or duplicate issues into one item in your final list, and including the species, any quantitative impacts if any, compensation measures if any, the location, and the source document and page of the information. Format your content as markdown, putting any locations, species, and quantities into bold, and creating hyperlinks from your document references from the title and URLs provided. Here is the text: \n\n
"""

# Cached function to establish a connection to Databricks
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=cfg.host,
        http_path=SQL_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )

# Function to execute a query and return results as a Pandas DataFrame
def run_query(query: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

# Cached function to load document reference table
@st.cache_data
def load_doc_reference_table():
    conn = get_connection()
    query = f"SELECT date, filename, pdf_url, title FROM {DOC_REFERENCE_TABLE} WHERE stage = 'Decision'"
    return run_query(query, conn)

# Function to submit feedback to the database
def submit_feedback(run_id, user, feedback, rating, feedback_table, conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            INSERT INTO {feedback_table}
            VALUES ('{run_id}', '{user}', '{feedback}', {rating})
        """)
        return "Feedback submitted successfully."

# Function to create necessary tables in the database
def create_tables(paragraph_table_name, document_table_name, project_table_name, conn):
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {paragraph_table_name} (
                run_id STRING, pdf_url_encoded STRING, title STRING, chunk_text STRING, 
                chunk_pages STRING, model_used STRING, prompt STRING, ai_assessment STRING
            )
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {document_table_name} (
                run_id STRING, pdf_url_encoded STRING, title STRING, document_summary STRING, 
                model_used STRING, prompt STRING
            )
        """)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {project_table_name} (
                run_id STRING, project_impacts STRING, model_used STRING, prompt STRING
            )
        """)

# Query construction functions for different stages of processing
def construct_query_1(run_id, paragraph_target_table, prompt, model, document_ids):
    documents = "'" + "', '".join(document_ids) + "'"
    return f"""
        INSERT INTO {paragraph_target_table}
        SELECT
            '{run_id}' AS run_id,
            REPLACE(pdf_url, " ", "%20") AS pdf_url_encoded,
            title,
            chunk_text,
            chunk_pages,
            '{model}' AS model_used,
            "{prompt}" AS prompt,
            ai_query('{model}', "{prompt}" || chunk_text) AS ai_assessment
        FROM marcell.marine_planning.decision_chunks
        WHERE filename IN ({documents})
    """

def construct_query_2(run_id, paragraph_target_table, document_target_table, prompt, model):
    return f"""
        INSERT INTO {document_target_table}
        WITH impactful_pages AS (
            SELECT
                *,
                "Page(s): " || chunk_pages || "\n\n" || ai_assessment AS ai_assessment_with_pages
            FROM {paragraph_target_table}
            WHERE ai_assessment NOT ILIKE "%<NO IMPACT IDENTIFIED>%" AND run_id = '{run_id}'
        ),
        numbered_impactful_pages AS (
            SELECT
                title,
                pdf_url_encoded,
                ai_assessment_with_pages,
                ROW_NUMBER() OVER (PARTITION BY title ORDER BY chunk_pages) AS row_num
            FROM impactful_pages
        ),
        grouped_impactful_pages AS (
            SELECT
                title || ' (part ' || CEIL(row_num / 200) || ')' AS title_part,
                pdf_url_encoded,
                ai_assessment_with_pages
            FROM numbered_impactful_pages
        )
        SELECT
            '{run_id}' AS run_id,
            pdf_url_encoded,
            title_part AS title,
            ai_query("{model}", system_prompt || concatenated_assessments) AS document_summary,
            '{model}' AS model_used,
            "{prompt}" AS prompt
        FROM (
            SELECT
                pdf_url_encoded,
                title_part,
                "{prompt}" AS system_prompt,
                concat_ws('"\n\n\n ==================" \n\n', collect_list(ai_assessment_with_pages)) AS concatenated_assessments
            FROM grouped_impactful_pages
            GROUP BY pdf_url_encoded, title_part
        );
    """

def construct_query_3(run_id, document_target_table, project_target_table, prompt, model):
    return f"""
        INSERT INTO {project_target_table}
        WITH doc_summaries AS (
            SELECT *, "Document: " || title || "\n" || "URL: " || pdf_url_encoded || "\n" || "Summary: " || document_summary AS doc_assessment_with_titles
            FROM {document_target_table}
            WHERE run_id = '{run_id}'
        )
        SELECT
            '{run_id}' AS run_id,
            ai_query("{model}", system_prompt || concatenated_doc_assessments) AS project_impacts,
            '{model}' AS model_used, "{prompt}" AS prompt
        FROM (
            SELECT "{prompt}" AS system_prompt,
            concat_ws('"\n\n\n ==================" \n\n', collect_list(doc_assessment_with_titles)) AS concatenated_doc_assessments
            FROM doc_summaries
        );
    """

# Function to fetch available run IDs from the backend
def fetch_run_ids(conn) -> list:
    query = f"SELECT DISTINCT run_id FROM {PROJECT_TARGET_TABLE} ORDER BY run_id DESC"
    df_run_ids = run_query(query, conn)
    return df_run_ids["run_id"].tolist()

# Function to fetch results for a specific run ID
def fetch_results_for_run(run_id, conn) -> pd.DataFrame:
    project_query = f"SELECT * FROM {PROJECT_TARGET_TABLE} WHERE run_id = '{run_id}'"
    project_results = run_query(project_query, conn)
    if project_results.empty:
        return None
    
    project_impacts = project_results["project_impacts"].values[0]
    prompt_3 = project_results["prompt"].values[0]
    model_3 = project_results["model_used"].values[0]

    document_query = f"SELECT pdf_url_encoded, title, prompt, model_used FROM {DOCUMENT_TARGET_TABLE} WHERE run_id = '{run_id}'"
    document_results = run_query(document_query, conn)
    document_titles = document_results["title"].tolist()
    document_urls = document_results["pdf_url_encoded"].tolist()
    prompt_2 = document_results["prompt"].values[0]
    model_2 = document_results["model_used"].values[0]

    paragraph_query = f"SELECT DISTINCT prompt, model_used FROM {PARAGRAPH_TARGET_TABLE} WHERE run_id = '{run_id}'"
    paragraph_results = run_query(paragraph_query, conn)
    prompt_1 = paragraph_results["prompt"].values[0]
    model_1 = paragraph_results["model_used"].values[0]

    return {
        "project_impacts": project_impacts,
        "prompt_1": prompt_1,
        "model_1": model_1,
        "prompt_2": prompt_2,
        "model_2": model_2,
        "prompt_3": prompt_3,
        "model_3": model_3,
        "document_titles": document_titles,
        "document_urls": document_urls
    }



# Load document reference table and display in Streamlit
doc_reference_df = load_doc_reference_table()
titles = doc_reference_df["title"].tolist()


# Main Streamlit app logic
conn = get_connection()

# Fetch available run IDs
run_ids = fetch_run_ids(conn)

# Display title and description
# Display the logo
logo = Image.open("assets/tce_logo.png")
st.image(logo, use_container_width=True)
st.title("Marine Planning Document Summarisation")
st.markdown("""
This app summarises the impacts of infrastructure development on marine life. It uses AI models to extract relevant information from documents.
""")
st.markdown("## Instructions")

# Add a dropdown to select a previous run or choose to run a new extraction
selected_run_id = st.selectbox("Select a previous run or choose to run a new extraction", ["Run New Extraction"] + run_ids)

if selected_run_id == "Run New Extraction":
    # If user chooses to run a new extraction, allow them to select documents
    selected_titles = st.multiselect("Select document titles", titles)
    selected_docs = doc_reference_df[doc_reference_df["title"].isin(selected_titles)]
    selected_doc_filenames = selected_docs.filename.tolist()
    st.dataframe(selected_docs[["date", "title", "pdf_url"]])

    # Allow users to edit prompts
    with st.expander("Edit Prompts", expanded=False):
        prompt_1 = st.text_area("Edit Prompt 1", value=prompt_1_default, height=200)
        prompt_2 = st.text_area("Edit Prompt 2", value=prompt_2_default, height=200)
        prompt_3 = st.text_area("Edit Prompt 3", value=prompt_3_default, height=200)

    # Run a new extraction process
    if st.button("Run New Extraction Process"):
        progress_bar = st.progress(0.0, "Starting extraction process")
        create_tables(PARAGRAPH_TARGET_TABLE, DOCUMENT_TARGET_TABLE, PROJECT_TARGET_TABLE, conn)
        run_id = CURRENT_USER + "_" + datetime.now().strftime("%Y%m%d%H%M%S")

        # Execute queries for each stage
        query_1 = construct_query_1(run_id, PARAGRAPH_TARGET_TABLE, prompt_1, MODEL_1, selected_doc_filenames)
        query_2 = construct_query_2(run_id, PARAGRAPH_TARGET_TABLE, DOCUMENT_TARGET_TABLE, prompt_2, MODEL_2)
        query_3 = construct_query_3(run_id, DOCUMENT_TARGET_TABLE, PROJECT_TARGET_TABLE, prompt_3, MODEL_3)

        _ = run_query(query_1, conn)
        progress_bar.progress(0.33, "Relevant paragraphs extracted")
        _ = run_query(query_2, conn)
        progress_bar.progress(0.67, "Individual documents summarised")
        _ = run_query(query_3, conn)
        progress_bar.progress(1.0, "Project impacts summarised")
        st.success("DONE")

        # Display final results
        results = fetch_results_for_run(run_id, conn)
        st.title(f"Result summary for run {run_id}")
        st.markdown(results["project_impacts"])
        with st.expander("View metadata", expanded=False):
            st.write("Documents summarised:")
            for title, url in zip(results["document_titles"], results["document_urls"]):
                st.markdown(f"* [{title}]({url})")
            st.write("Prompts and models used:")
            st.write(f"Prompt 1: {results['prompt_1']}\nModel 1: {results['model_1']}")
            st.write(f"Prompt 2: {results['prompt_2']}\nModel 2: {results['model_2']}")
            st.write(f"Prompt 3: {results['prompt_3']}\nModel 3: {results['model_3']}")
        
        with st.expander("Provide feedback", expanded=False):
            st.write("Please provide feedback on the results of the extraction process.")
            feedback = st.text_input("Feedback", "")
            rating = st.slider("Rating (1-5)", 1, 5)
            if st.button("Submit Feedback"):
                result = submit_feedback(run_id, CURRENT_USER, feedback, rating, SUMMARY_FEEDBACK_TABLE, conn)
                st.info(result)

        progress_bar.empty()
else:
    # Load and display results for the selected run ID
    st.write(f"Loading results for run ID: {selected_run_id}")
    results = fetch_results_for_run(selected_run_id, conn)
    if not results:
        st.warning("No results found for the selected run ID.")
    else:
        st.title(f"Result summary for run {selected_run_id}")
        st.markdown(results["project_impacts"])
        with st.expander("View metadata", expanded=False):
            st.write("Documents summarised:")
            for title, url in zip(results["document_titles"], results["document_urls"]):
                st.markdown(f"* [{title}]({url})")
            st.write("Prompts and models used:")
            st.write(f"Prompt 1: {results['prompt_1']}\nModel 1: {results['model_1']}")
            st.write(f"Prompt 2: {results['prompt_2']}\nModel 2: {results['model_2']}")
            st.write(f"Prompt 3: {results['prompt_3']}\nModel 3: {results['model_3']}")

        with st.expander("Provide feedback", expanded=False):
            st.write("Please provide feedback on the results of the extraction process.")
            feedback = st.text_input("Feedback", "")
            rating = st.slider("Rating (1-5)", 1, 5)
            if st.button("Submit Feedback"):
                result = submit_feedback(selected_run_id, CURRENT_USER, feedback, rating, SUMMARY_FEEDBACK_TABLE, conn)
                st.info(result)


