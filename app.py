import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
import pandas as pd
import snowflake.connector
import altair as alt
import pymsteams
from datetime import datetime
import json
import os

if "ctx" not in st.session_state:
    st.session_state.ctx = None

st.set_page_config(page_title="Monitoring Alerts", page_icon="🔔", layout="wide")
st.title("🔔 Monitoring Data Anomaly Alerts")

my_teams_message = pymsteams.connectorcard(
    "https://trimedxllc.webhook.office.com/webhookb2/993abbd8-1479-454b-8704-38758b61393d@bab4504a-c9f6-4480-af12-6ee6b44db482/IncomingWebhook/70a50de8021f44879640ec2147e63b39/46ed046b-d2d6-46ec-955f-6a1229444c2c"
)

# Makes interactive table
def aggrid_interactive_table(df: pd.DataFrame):
    options = GridOptionsBuilder.from_dataframe(df)
    options.configure_side_bar()
    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme="streamlit",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )
    return selection


# Snowflake connector
@st.experimental_singleton
def connect_to_snowflake(user):
    return snowflake.connector.connect(
        user=user,
        authenticator="externalbrowser",
        account=os.getenv("account"),
        database=os.getenv("database"),
        warehouse=os.getenv("warehouse"),
        schema=os.getenv("schema"),
    ) 


def query(sql, action="read"):
    with st.session_state.ctx.cursor() as cs:

        cs.execute(sql)
        if action == "read":
            data = cs.fetch_pandas_all()
        else:
            data = "done!"

    return data


class Alert:

    def __init__(self, alert, item):
        self.alert = alert
        self.item = item
        self.snapshot_date = self.item["SNAPSHOT_DATE"]
        self.schema_name = self.item["SCHEMA_NAME"]
        self.object_name = self.item["OBJECT_NAME"]
        self.table = f"{self.schema_name}.{self.object_name}"
        self.database_name = item["DATABASE_NAME"]
        self.object_type = self.item["OBJECT_TYPE"]
        self.sf_updated_timestamp = self.item["SF_UPDATED_TIMESTAMP"]
        self.sf_updated_user = self.item["SF_UPDATED_USER"]
        self.error_message = self.item["ERROR_MESSAGE"]
        self.post_init()

    def post_init(self):
        pass

    def get_chart_data(self, days_ago):
        pass

    def display_chart(self, days_ago):
        pass

    def push_to_snowflake(self, txt):
        alert_payload=json.dumps({'SNAPSHOT_DATE': self.snapshot_date
                        , 'SCHEMA_NAME': self.schema_name
                        , 'OBJECT_NAME': self.object_name
                        , 'OBJECT_TYPE': self.object_type
                        , 'ALERT_TYPE': self.alert
                        , 'ERRROR_MESSAGE': self.error_message
                        , 'SF_UPDATED_TIMESTAMP': self.sf_updated_timestamp
                        , 'SF_UPDATED_USER': self.sf_updated_user
                        , 'NOTES': txt if txt else None
                    }).replace('$$', '\$\$')
        query(f"""insert into MONITORING.RPT_ALERT_NOTES select parse_json($${alert_payload}$$), SYSDATE(), 'STREAMLIT_MONITORING';""", 'write')
        return st.success("Response recorded in Snowflake.")

    def create_teams_message(self, txt):
        my_teams_message.summary("Test Message")
        my_teams_message.color("#FF5600")
        my_teams_message.addLinkButton(
            "Streamlit App", "http://localhost:8501/"
        )

        # Create the section
        my_message_section = pymsteams.cardsection()
        my_message_section.title(f"New Monitoring Comment: For {self.alert} Issue Detection")
        my_message_section.activitySubtitle(self.table)

        # Section Text
        my_message_section.addFact("Comment:", txt)
        my_message_section.addFact("Snapshot Date:", self.snapshot_date)

        content = self.teams_content(my_message_section)
        if content:
            my_message_section = content 

        # Section Images
        my_teams_message.addSection(my_message_section)
        my_teams_message.send()

        return st.info("Response recorded in Teams.")

    def teams_content(self, message_section):
        pass

    def create_response_section(self):
        txt = st.text_area("Notes")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("All Clear - Post to Snowflake"):
                self.push_to_snowflake(txt)
        with col2:
            if st.button("Issue - Post to Teams"):
                self.create_teams_message(txt)
                self.push_to_snowflake(txt)

class DataGrowthAlert(Alert):

    def post_init(self):
        self.error_message = json.loads(self.error_message)  
        self.row_count = self.error_message["ROW_COUNT"]
        self.prev_row_count = self.error_message["PREV_ROW_COUNT"]
        self.diff_percentage = self.error_message["DIFF_PERCENTAGE"]

    def get_chart_data(self, days_ago):
        df = query(
            f"""SELECT SNAPSHOT_DATE, ERROR_MESSAGE:ROW_COUNT as ROW_COUNT, ERROR_MESSAGE:DIFF_PERCENTAGE as DIFF_PERCENTAGE, CASE WHEN ERROR_MESSAGE:DIFF_PERCENTAGE > 25 or ERROR_MESSAGE:DIFF_PERCENTAGE < -25 THEN 1 ELSE 0 END AS ISSUE
            FROM MART_MONITORING.RPT_ALERTS
            WHERE SCHEMA_NAME || '.'|| OBJECT_NAME = '{self.table}' AND SNAPSHOT_DATE > dateadd('day', {days_ago}, current_date());"""
        )
        return df
    
    def display_chart(self, days_ago):
        df = self.get_chart_data(days_ago)
        c = (
            alt.Chart(df, title="ROW COUNT vs. SNAPSHOT DATE")
            .mark_bar()
            .encode(
                alt.X("yearmonthdate(SNAPSHOT_DATE):O", title="SNAPSHOT DATE"),
                alt.Y("ROW_COUNT:Q", title='ROW COUNT'),
                color="ISSUE",
                tooltip=["SNAPSHOT_DATE", "ROW_COUNT", "DIFF_PERCENTAGE"],
            )
        )
        return st.table(df), st.altair_chart(c, use_container_width=True)

    def teams_content(self, message_section):
        message_section.addFact("Row Count:", self.row_count)
        message_section.addFact("Previous Row Count:", self.prev_row_count)
        message_section.addFact("Difference Percentage:", f"{self.diff_percentage}%")
        return message_section

class KeyViewTestAlert(Alert):

    def post_init(self):
        try:
            self.error_message = json.loads(json.loads(self.item["ERROR_MESSAGE"])["ERROR_MESSAGE"])
        except:
            self.error_message = json.loads(self.item["ERROR_MESSAGE"])["ERROR_MESSAGE"]

    def get_chart_data(self, days_ago):
        df = query(
            f"""SELECT SNAPSHOT_DATE, OBJECT_NAME 
            FROM MART_MONITORING.RPT_ALERTS
            WHERE SCHEMA_NAME || '.'|| OBJECT_NAME = '{self.table}' AND SNAPSHOT_DATE > dateadd('day', {days_ago}, current_date());"""
        )
        return df

    def display_chart(self,days_ago):
        df = self.get_chart_data(days_ago)
        c = (
            alt.Chart(df, title="OBJECT FAILURE RATE vs. SNAPSHOT DATE")
            .mark_bar()
            .encode(
                alt.X("yearmonthdate(SNAPSHOT_DATE):O", title="SNAPSHOT DATE"),
                alt.Y("count(OBJECT_NAME)", title="OBJECT FAILURE RATE"),
                tooltip=["SNAPSHOT_DATE", "OBJECT_NAME", "count(OBJECT_NAME)"],
            )
        )
        return st.altair_chart(c, use_container_width=True)

    def teams_content(self, message_section):
        if type(self.error_message) != str:
            log_test_errors = []
            for test_error in self.error_message:
                log_test_errors.append(f"<br> There are {test_error[0]} records with {test_error[1]} key")
            message_section.text(f"<strong>Error Message:</strong> {' '.join(str(test_error) for test_error in log_test_errors)}")  
        else:
            message_section.text(f"<strong>Error Message:</strong><br>{self.error_message}")
        return message_section

class TaskHistoryAlert(Alert):

    def post_init(self):
        print(self.error_message)
        error_message = json.loads(self.error_message)
        self.state = error_message["STATE"]
        self.query_text = error_message["QUERY_TEXT"]
        self.error_code = error_message["ERROR_CODE"]
        self.error_message= error_message["ERROR_MESSAGE"]

    def get_chart_data(self, days_ago):
        df = query(
            f"""SELECT SNAPSHOT_DATE, OBJECT_NAME
            FROM MART_MONITORING.RPT_ALERTS
            WHERE SCHEMA_NAME || '.'|| OBJECT_NAME = '{self.table}' AND SNAPSHOT_DATE > dateadd('day', {days_ago}, current_date());"""
        )
        return df
    
    def display_chart(self, days_ago):
        df = self.get_chart_data(days_ago)
        c = (
            alt.Chart(df, title="TASK FAILURE RATE vs. SNAPSHOT DATE")
            .mark_bar()
            .encode(
                alt.X("yearmonthdate(SNAPSHOT_DATE):O", title="SNAPSHOT DATE"),
                alt.Y("count(OBJECT_NAME)", title="TASK FAILURE RATE"),
                tooltip=["SNAPSHOT_DATE", "OBJECT_NAME", "count(OBJECT_NAME)"],
            )
        )
        return st.altair_chart(c, use_container_width=True)

    def teams_content(self, message_section):
        message_section.addFact("Query Text:", self.query_text)
        message_section.addFact("Error Code:", self.error_code)
        message_section.addFact("State:", self.state)
        message_section.text(f"<strong>Error Message:</strong><br>{self.error_message}")

        return message_section


# Pulls RPT_ALERTS table
def get_main(d1, d2, alert):
    df = query(
        f"""SELECT * FROM MART_MONITORING.RPT_ALERTS
        WHERE SNAPSHOT_DATE >= '{d1}'::date AND SNAPSHOT_DATE <= '{d2}'::date AND ALERT_TYPE ='{alert}';"""
    )
    return df

def snowflake_date(date):
    return date.strftime("%Y-%m-%d")

def login():
    with st.form("my_form"):
        username = st.text_input("Snowflake Username")

        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.info("Username Authenticated! Press Submit to Log In")
            return connect_to_snowflake(username)

def homepage():

    alerts = {
        "DATA GROWTH": DataGrowthAlert,
        "KEY TEST": KeyViewTestAlert,
        "VIEW TEST": KeyViewTestAlert,
        "TASK HISTORY": TaskHistoryAlert
    }

    # Widgets
    col1, col2, col3 = st.columns(3)
    with col1:
        d1 = st.date_input("Start Date", datetime.now())
    with col2:
        d2 = st.date_input("End Date", datetime.now())
    with col3:
        alert = st.selectbox("Alert Type", alerts.keys())

    # Alerts table within date range
    sd1 = snowflake_date(d1)
    sd2 = snowflake_date(d2)
    main_table = get_main(sd1, sd2, alert)

    selection = aggrid_interactive_table(df=main_table)
    if selection:
        for item in selection["selected_rows"]:
            days_ago = st.slider("Days ago", -365, 1, -200)

            alert_obj = alerts[alert](alert, item)

            alert_obj.display_chart(days_ago)
            alert_obj.create_response_section()

if __name__ == "__main__":
    if not st.session_state.ctx:

        st.session_state.ctx = login()

    elif st.session_state.ctx:

        homepage()
