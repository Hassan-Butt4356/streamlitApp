import streamlit as st
import pandas as pd


wkday_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkDAY Route Comparison')
wkday_dir_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkDAY Route DIR Comparison')
wkend_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkEND Route Comparison')
wkend_dir_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkEND Route DIR Comparison')

wkend_time_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkEND Time Data')
wkday_time_df=pd.read_excel('data/reviewtool_20241224_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx',sheet_name='WkDAY Time Data')

detail_df=pd.read_excel('data/details_vta_CA_od_excel.xlsx',sheet_name='TOD')

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

# final_usage = st.sidebar.selectbox(label='Final Usage', options=['All', 'Use', 'Remove'])

    # unique_dates = df1['Date'].dt.date.unique()
    # selected_date = st.sidebar.date_input(
    #     "Select a date",
    #     value=None,
    #     min_value=min(unique_dates),
    #     max_value=max(unique_dates)
    # )
# else:
#     final_usage = 'All'  # Default value when sidebar is not shown
#     search_query=''


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


    st.dataframe(details_df[['OPPO_TIME[CODE]', 'TIME_ON[Code]', 'TIME_ON', 'TIME_PERIOD[Code]',
                              'TIME_PERIOD', 'START_TIME']], height=670, use_container_width=True)

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
        st.dataframe(filtered_df1, height=690)

    # Display buttons and dataframes in the second column (col2)
    with col2:
        st.subheader("Time Range Data")
        filtered_df2 = filter_dataframe(data2, search_query)
        st.dataframe(filtered_df2, height=300,use_container_width=True)

        st.subheader("Route Level Comparison")
        filtered_df3 = filter_dataframe(data3, search_query)
        st.dataframe(filtered_df3, height=300,use_container_width=True)


def weekday_page():
    st.title("Weekday OverAll Data")
    main_page(wkday_dir_df[['ROUTE_SURVEYEDCode','(0) Goal', '(1) Goal',
       '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal', '(0) Collect',
       '(1) Collect', '(2) Collect', '(3) Collect', '(4) Collect',
       '(5) Collect', '(0) Remain', '(1) Remain', '(2) Remain',
       '(3) Remain', '(4) Remain', '(5) Remain']], wkday_time_df[['Display_Text','Original Text','Time Range',0, 1, 2, 3, 4, 5]], wkday_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Load weekday data
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
       '(3) Remain', '(4) Remain', '(5) Remain']], wkend_time_df[['Display_Text','Original Text','Time Range',0, 1, 2, 3, 4, 5]], wkend_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Load weekend data
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
       '(3) Remain', '(4) Remain', '(5) Remain']], wkday_time_df[['Display_Text','Original Text','Time Range',0, 1, 2, 3, 4, 5]], wkday_df[['ROUTE_SURVEYEDCode', 'Route Level Goal', '# of Surveys', 'Remaining']])  # Default to original data for the main page
