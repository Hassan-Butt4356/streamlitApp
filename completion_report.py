import streamlit as st
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer,write_pandas
from decouple import config

# print()
def create_snowflake_connection():
    # Replace with your Snowflake credentials
    conn = snowflake.connector.connect(
        user=config('user'),  # Snowflake username
        password=config('password'),  # Snowflake password
        account=config('account'),  # Snowflake account (e.g., abc12345.us-east-1)
        warehouse=config('warehouse'),  # Snowflake warehouse name
        database=config('database'),  # Your Snowflake database name
        schema=config('schema') , # Default schema (you can change it)
        role=config('role')  # Your Snowflake database name
    )
    return conn


# def style_dataframe(df):
#     """
#     Applies conditional formatting to a dataframe.
#     Colors cells based on their values:
#     - Green for values >= -10000 and < 1
#     - Yellow for values >= 1 and < 6
#     - Pink for values >= 6 and < 35
#     - Red for values >= 35 and < 10000
#     """
#     numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
#     def highlight_cell(val):
#         if -10000 <= val < 1:
#             return "background-color: green; color: white;"
#         elif 1 <= val < 6:
#             return "background-color: yellow; color: black;"
#         elif 6 <= val < 35:
#             return "background-color: pink; color: black;"
#         elif 35 <= val < 10000:
#             return "background-color: red; color: white;"
#         return ""
    
#     def style_numeric_columns(df, numeric_columns):
#         """
#         Style numeric columns based on their values.
#         """
#         return df.style.applymap(
#             highlight_cell, subset=numeric_columns
#         )

#     styled_df = style_numeric_columns(df, numeric_columns)
#     # Apply the style function to all cells in the dataframe
#     return styled_df

def style_dataframe(df, column_name_patterns):
    """
    Applies conditional formatting to a dataframe.
    Colors cells in the specified columns based on their values:
    - Green for values >= -10000 and < 1
    - Yellow for values >= 1 and < 6
    - Pink for values >= 6 and < 35
    - Red for values >= 35 and < 10000

    Parameters:
    - df: pandas DataFrame to style.
    - column_name_patterns: list of strings or substrings to filter column names (e.g., ["Remain"]).
    """
    # Filter columns based on the given patterns
    target_columns = [col for col in df.columns if any(pattern in col for pattern in column_name_patterns)]
    
    def highlight_cell(val):
        if -10000 <= val < 1:
            return "background-color: #BCE29E; color: black;"

        elif 1 <= val < 6:
            return "background-color: #E5EBB2; color: black;"

        elif 6 <= val < 35:
            return "background-color: #F8C4B4; color: black;"

        elif 35 <= val < 10000:
            return "background-color: #FF8787; color: black;"

        return ""

    # Apply styling only to the target columns
    return df.style.applymap(
        highlight_cell, subset=target_columns
    )

column_name_patterns=['(0) Remain', '(1) Remain', '(2) Remain', 
       '(3) Remain', '(4) Remain', '(5) Remain' ,'Remaining']

def fetch_dataframes_from_snowflake():
    """
    Fetches data from Snowflake tables and returns them as a dictionary of DataFrames.

    Returns:
        dict: A dictionary where keys are DataFrame names and values are DataFrames.
    """
    # Snowflake connection details
    conn = create_snowflake_connection()
    cur = conn.cursor()

    # Table-to-DataFrame mapping
    table_to_df_mapping = {
        'wkday_comparison': 'wkday_df',
        'wkday_dir_comparison': 'wkday_dir_df',
        'wkend_comparison': 'wkend_df',
        'wkend_dir_comparison': 'wkend_dir_df',
        'wkend_time_data': 'wkend_time_df',
        'wkday_time_data': 'wkday_time_df',
        'TOD': 'detail_df'
    }

    # Initialize an empty dictionary to hold DataFrames
    dataframes = {}

    try:
        # Loop through each table, fetch its data, and store it in the corresponding DataFrame
        for table_name, df_name in table_to_df_mapping.items():
            # Query to fetch data
            query = f"SELECT * FROM {table_name}"
            
            # Execute query and fetch data
            cur.execute(query)
            data = cur.fetchall()
            
            # Get column names from the cursor description
            columns = [desc[0] for desc in cur.description]
            
            # Convert data to DataFrame
            df = pd.DataFrame(data, columns=columns)
            dataframes[df_name] = df
            
            print(f"Data fetched and stored in DataFrame: {df_name}")
    except Exception as e:
        print(f"Error fetching data: {e}")
    finally:
        # Close cursor and connection
        cur.close()
        conn.close()

    return dataframes

# Fetch dataframes from Snowflake
dataframes = fetch_dataframes_from_snowflake()

# Example: Access DataFrames
wkday_df = dataframes['wkday_df']
wkday_dir_df = dataframes['wkday_dir_df']
wkend_df = dataframes['wkend_df']
wkend_dir_df = dataframes['wkend_dir_df']
wkend_time_df = dataframes['wkend_time_df']
wkday_time_df = dataframes['wkday_time_df']
detail_df = dataframes['detail_df']


st.set_page_config(page_title="DataFrames Example", layout='wide')


def download_csv(csv):
    # Use io.BytesIO to create a downloadable link
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="new_data.csv",
        mime="text/csv"
    )


def create_csv(df):
    # Convert the DataFrame to CSV
    csv = df.to_csv(index=False)
    return csv

# Check the query parameter to determine which page to show
query_params = st.experimental_get_query_params()
page = query_params.get("page", ["main"])[0]

# Show sidebar only on the main page
# if page == "main":
if page!='timedetails':
    st.sidebar.header("Filters")
    search_query=st.sidebar.text_input(label='Search', placeholder='Search')


header_col1, header_col2, header_col3 = st.columns([2, 2,1]) 

with header_col1:
    st.header('Completion Report')


with header_col2:
    if page!='timedetails':
        if page == "weekend":
            st.header(f'Total Records: {wkend_df["# of Surveys"].sum()}')
        else:  # Default to weekday data for main and weekday pages
            st.header(f'Total Records: {wkday_df["# of Surveys"].sum()}')
        if st.button('Time OF Day Details'):
            st.experimental_set_query_params(page="timedetails")
            st.experimental_rerun()
    else:
        st.header(f'Time OF Day Details')

with header_col3:
    if st.button("WEEKDAY-OVERALL"):
        st.experimental_set_query_params(page="weekday")
        st.experimental_rerun()
    if st.button("WEEKEND-OVERALL"):
        st.experimental_set_query_params(page="weekend")
        st.experimental_rerun()


def filter_dataframe(df, query):
    if query:
        df = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]
    # if usage.lower() == 'use':
    #     df = df[df['Final_Usage'].str.lower() == 'use']
    # elif usage.lower() == 'remove':
    #     df = df[df['Final_Usage'].str.lower() == 'remove']
    
    # if date:
    #     df = df[df['Date'].dt.date == date]

    return df

def time_details(details_df):
    # st.dataframe(details_df[['OPPO_TIME[CODE]', 'TIME_ON[Code]', 'TIME_ON', 'TIME_PERIOD[Code]',
    #                           'TIME_PERIOD', 'START_TIME']], height=670, use_container_width=True)

    st.dataframe(details_df, height=670, use_container_width=True)
    if st.button("GO TO HOME"):
        st.experimental_set_query_params(page="main")
        st.experimental_rerun()

def main_page(data1, data2, data3):
    """Main page display with dynamic data"""
    # Create two columns layout
    col1, col2 = st.columns([2, 1])  # Left column is wider

    # Display the first dataframe on the left full screen (col1)
    with col1:
        if page=='main':
            st.subheader('Route Direction Level Comparison (WeekDAY)')
        else:
            st.subheader("Route Direction Level Comparison")
        filtered_df1 = filter_dataframe(data1, search_query)
        st.dataframe(style_dataframe(filtered_df1,column_name_patterns), height=690)

    # Display buttons and dataframes in the second column (col2)
    with col2:
        st.subheader("Time Range Data")
        filtered_df2 = filter_dataframe(data2, search_query)
        st.dataframe(filtered_df2, height=300,use_container_width=True)

        st.subheader("Route Level Comparison")
        filtered_df3 = filter_dataframe(data3, search_query)
        st.dataframe(style_dataframe(filtered_df3,column_name_patterns), height=300,use_container_width=True)


def weekday_page():
    st.title("Weekday OverAll Data")
    main_page(wkday_dir_df[['ROUTE_SURVEYEDCode','(0) Goal', '(1) Goal',
       '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal', '(0) Collect',
       '(1) Collect', '(2) Collect', '(3) Collect', '(4) Collect',
       '(5) Collect', '(0) Remain', '(1) Remain', '(2) Remain',
       '(3) Remain', '(4) Remain', '(5) Remain']], wkday_time_df[['Display_Text','Original Text','Time Range','0', '1', '2', '3', '4', '5']], wkday_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Load weekday data
    # csv = create_csv(wkday_df)
    # download_csv(csv)

    if st.button("GO TO HOME"):
        st.experimental_set_query_params(page="main")
        st.experimental_rerun()


def weekend_page():
    st.title("Weekend OverAll Data")
    main_page(wkend_dir_df[['ROUTE_SURVEYEDCode','(0) Goal', '(1) Goal',
       '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal', '(0) Collect',
       '(1) Collect', '(2) Collect', '(3) Collect', '(4) Collect',
       '(5) Collect', '(0) Remain', '(1) Remain', '(2) Remain',
       '(3) Remain', '(4) Remain', '(5) Remain']], wkend_time_df[['Display_Text','Original Text','Time Range','0', '1','2', '3', '4', '5']], wkend_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Load weekend data
    # csv = create_csv(wkend_df)
    # download_csv(csv)

    if st.button("GO TO HOME"):
        st.experimental_set_query_params(page="main")
        st.experimental_rerun()


if page == "weekday":
    weekday_page()
elif page == "weekend":
    weekend_page()
elif page=='timedetails':
    time_details(detail_df)
else:
    main_page(wkday_dir_df[['ROUTE_SURVEYEDCode','(0) Goal', '(1) Goal',
       '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal', '(0) Collect',
       '(1) Collect', '(2) Collect', '(3) Collect', '(4) Collect',
       '(5) Collect', '(0) Remain', '(1) Remain', '(2) Remain',
       '(3) Remain', '(4) Remain', '(5) Remain']], wkday_time_df[['Display_Text','Original Text','Time Range','0', '1', '2', '3', '4', '5']], wkday_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Default to original data for the main page
