def app():
    import streamlit as st
    import pandas as pd
    import numpy as np
    from google.cloud import bigquery

    # Authenticate with Google Cloud using a service account
    service_account_path = '/Users/diegolozano/Desktop/CloudK Business Case/ck-sp-case-study-2024-2aef28547e77.json'
    client = bigquery.Client.from_service_account_json(service_account_path)

    # Query BigQuery tables
    ordersdata_query = """SELECT * FROM `ck-sp-case-study-2024.case_study.orders`"""
    hoursdata_query = """SELECT * FROM `ck-sp-case-study-2024.case_study.labor_hours`"""

    # Set page configuration for full-width test
    st.set_page_config(layout="wide")

    ordersdata = client.query(ordersdata_query).to_dataframe()
    hoursdata = client.query(hoursdata_query).to_dataframe()
    filtered_ordersdata = ordersdata[ordersdata["is_cancelled"] != True]
    orders_per_day_per_facility = ordersdata.groupby(["DATE","Facility_id"]).sum()
    hoursdata = hoursdata.rename(columns={
        'date': 'DATE',
        'facility_id': 'Facility_id',
        'facility_name': 'Facility',
        'labor_hours_actual_including_cr_hours_allocation': 'Labor_hours_actual',
        'daily_cr_labor_hours_allocation': 'Daily_cr_labor_hours_allocation'
    })
    name_facilities = hoursdata.groupby("Facility_id").agg({
        "Facility": "first"
    }).reset_index()

    #Tables for Question1
    filtered_ordersdata = ordersdata[ordersdata["is_cancelled"] != True]
    question1 = filtered_ordersdata.groupby('organization_name')['GMV_Minus_Discount',].sum().reset_index()
    question1 = question1.sort_values(by='GMV_Minus_Discount', ascending=False)

    #Tables for Question 2
    question2 = ordersdata.groupby(by="organization_name")[["GMV", "Orders"]].mean().reset_index()

    #Tables for Question 3
    ordersperfacility = ordersdata.groupby(["DATE", "Facility_id", "Facility"])["Orders"].sum()
    hoursperfacility = hoursdata.groupby(["DATE", "Facility_id", 'Facility'])["Labor_hours_actual"].sum()
    question3 = pd.merge(ordersperfacility, hoursperfacility, on=["Facility_id", 'Facility'])
    question3 = question3.groupby(["Facility"]).mean()
    question3["orders_per_hour"] = question3["Orders"] / 12

    #Tables for Question 4
    hoursdata[("Labor_Cost")] = hoursdata [("Labor_hours_actual")] * 18
    question4 = hoursdata.groupby("Facility_id").agg({
        'Facility': 'first',
        'Labor_hours_actual': 'mean',
        'Labor_Cost': 'mean'
    }).reset_index()

    #Tables for question 5
    question5 = pd.merge(orders_per_day_per_facility, hoursdata, on=["DATE", 'Facility_id'])
    question5 = question5[question5['Labor_hours_actual'] != 0]
    question5['average_order_per_labour_hour'] = question5['Orders'] / question5['Labor_hours_actual']
    question5 = question5.groupby("Facility_id").agg({
        'average_order_per_labour_hour': 'mean',
        
    }).reset_index()
    question5 = pd.merge(name_facilities, question5, on='Facility_id')

    #Tables for Question6
    facility_names = ordersdata.groupby("Facility_id")["Facility"].first().reset_index()
    question6 = ordersdata.copy()
    question6['Processing_Revenue'] = question6['GMV'] * 0.04
    question6 = question6.groupby('Facility_id')[['Processing_Revenue']].mean().reset_index()

    average_order_per_day_per_facility = orders_per_day_per_facility.groupby("Facility_id")["Orders"].mean().reset_index()
    question6 = pd.merge(average_order_per_day_per_facility, question6, on=["Facility_id"])

    question6['average_processing_revenue_per_facility'] = question6['Orders'] * question6['Processing_Revenue']
    ordersdata.loc[:, 'Processing_Revenue'] = ordersdata['GMV'] * 0.04

    facility_names = ordersdata.groupby("Facility_id")["Facility"].first().reset_index()
    question6 = pd.merge(average_order_per_day_per_facility, question6, on=["Facility_id"])
    question6 = pd.merge(facility_names, question6, on="Facility_id")
    #Tables for Question7

    # Create a copy of ordersdata to avoid the SettingWithCopyWarning
    question7_copy = ordersdata.copy()

    # Calculate processing revenue for each order
    question7_copy.loc[:, 'Processing_Revenue'] = question7_copy['GMV'] * 0.04

    # Group by 'Facility_id' and 'DATE' and sum the processing revenue
    sum_processing_revenue = question7_copy.groupby(["DATE", "Facility_id"])['Processing_Revenue'].sum().reset_index()

    # Calculate cost labor hours per day
    hoursdata['Labor_Cost'] = hoursdata['Labor_hours_actual'] * 18

    # Group by 'Facility_id' and 'DATE' and sum the processing revenue and cost labor
    sum_labor_hours = hoursdata.groupby(["DATE", "Facility_id"])['Labor_Cost'].sum().reset_index()

    # Merge both sums per Facility_id and DATE to analyze day by day
    question7_result = pd.merge(sum_processing_revenue, sum_labor_hours, on=["Facility_id", 'DATE'])

    # Update 'Processing_Revenue' based on the condition
    question7_result.loc[:, 'Processing_Revenue'] = np.where(question7_result['Labor_Cost'] == 0, 0, question7_result['Processing_Revenue'])

    # Calculate net cost
    question7_result[("net_cost")] = question7_result['Processing_Revenue'] - question7_result['Labor_Cost']

    # Group by 'Facility_id' and sum the values
    question7_final = question7_result.groupby('Facility_id').agg({
        'Processing_Revenue': 'sum',
        'Labor_Cost': 'sum',
        'net_cost': 'sum'
    }).reset_index()
    question7_final['Losing / Earning'] = np.where(question7_final['net_cost'] < 0, 'Losing', 'Earning')
    facility_names = ordersdata.groupby("Facility_id")["Facility"].first().reset_index()
    question7_final = pd.merge(facility_names, question7_final, on="Facility_id")
    #Net cost per days
    staffed = ordersdata.copy()

    # Calculate processing revenue for each order
    staffed.loc[:, 'Processing_Revenue'] = staffed['GMV'] * 0.04

    # Group by 'Facility_id' and 'DATE' and sum the processing revenue

    sum_processing_revenue = staffed.groupby(["DATE", "Facility_id"])[['Processing_Revenue', 'Orders']].sum().reset_index()

    # Calculate cost labor hours per day
    hoursdata['Labor_Cost'] = hoursdata['Labor_hours_actual'] * 18

    # Group by 'Facility_id' and 'DATE' and sum the processing revenue and cost labor
    sum_labor_hours = hoursdata.groupby(["DATE", "Facility_id"])[['Labor_Cost', 'Labor_hours_actual']].sum().reset_index()

    # Merge both sums per Facility_id and DATE to analyze day by day
    staffed_result = pd.merge(sum_processing_revenue, sum_labor_hours, on=["Facility_id", 'DATE'])

    # Update 'Processing_Revenue' based on the condition
    staffed_result.loc[:, 'Processing_Revenue'] = np.where(staffed_result['Labor_Cost'] == 0, 0, staffed_result['Processing_Revenue'])

    # Calculate net cost
    staffed_result[("net_cost")] = staffed_result['Processing_Revenue'] - staffed_result['Labor_Cost']
    staffed_result['Staffed'] = np.where((staffed_result['Orders'] /staffed_result['Labor_hours_actual'])<35, 'Overstaffed', np.where((staffed_result['Orders'] /staffed_result['Labor_hours_actual'])<45, 'Right', 'Understaffed'))
    staffed_result = pd.merge(staffed_result, question7_final, on="Facility_id")

    #Tables for Question 8
    # Create a copy of orders_per_day_per_facility to avoid unintended modifications
    question8 = ordersdata.copy()
    sum_processing_revenue8 = question8.groupby(["DATE", "Facility_id"])[['Processing_Revenue', 'Orders']].sum().reset_index()
    question8 = pd.merge(sum_processing_revenue8, hoursdata, on = ["Facility_id", "DATE"])
    question8["Optimized_hours"] = question8['Orders'] / 35
    question8["Optimized_Labor_Cost"] = question8['Optimized_hours'] * 18
    question8.loc[:, 'Processing_Revenue'] = np.where(question8['Labor_Cost'] == 0, 0, question8['Processing_Revenue'])
    question8[("net_cost")] = question8['Processing_Revenue'] - question8['Labor_Cost']
    question8[("optimized_net_cost")] = question8['Processing_Revenue'] - question8['Optimized_Labor_Cost']
    question8 = question8.groupby("Facility_id").agg({
        'Processing_Revenue': 'sum',
        'Orders': 'sum',
        'Labor_hours_actual': 'sum',
        'Labor_Cost': 'sum',
        'Optimized_hours': 'sum',
        'Optimized_Labor_Cost': 'sum',
        'net_cost': 'sum',
        'optimized_net_cost': 'sum'
    }).reset_index()

    total_net_cost = question7_final['net_cost'].sum()
    total_optimized_net_cost = question8['Optimized_Labor_Cost'].sum()

    # Display the totals
    print("Total Net Cost:", total_net_cost)
    print("Total Optimized Net Cost:", total_optimized_net_cost)
    incremental_income = total_optimized_net_cost - total_net_cost

    # Streamlit App

    # Import 
    import streamlit as st
    import subprocess
    subprocess.call(['pip', 'install', 'google-cloud-bigquery'])
    from google.cloud import bigquery
    import pandas as pd
    import numpy as np

    st.title('CloudKitchens Case Study - Dashboard by Diego Lozano Vega')

    # Question1
    st.subheader('1. What are the most successful restaurants across the country?')

    # Create a slider for selecting top N organizations
    top_n_question1 = st.slider('Select Top N Organizations (Question 1):', 1, len(question1), 5)

    sorted_question1 = question1.sort_values(by='GMV_Minus_Discount', ascending=False)

    # Create a bar chart for GMV_Minus_Discount
    st.bar_chart(sorted_question1.set_index('organization_name').head(top_n_question1)['GMV_Minus_Discount'])

    # Question2
    st.subheader('2. Average of performance')

    # Select metric
    selected_metric = st.selectbox('Select Metric:', ['GMV', 'Orders'])

    # Create a top N slider for selecting organizations
    top_n_question2 = st.slider('Select Top N Organizations (Question 2):', 1, len(question2), 5)

    # Create a list for the "subregion" column in ordersdata
    subregion_options = ordersdata['subregion'].unique().tolist()

    # Add a checkbox for selecting all subregions
    select_all_subregions = st.checkbox('Select All Subregions', False)

    if select_all_subregions:
        selected_subregions = subregion_options
    else:
        # Allow multiple selections in the subregion list
        selected_subregions = st.multiselect('Select Subregions:', subregion_options)

    # Filter data based on the selected subregions
    filtered_data = ordersdata[ordersdata['subregion'].isin(selected_subregions)]

    # Get the top N organizations based on the selected metric
    top_n_df = filtered_data.groupby(by="organization_name")[["GMV", "Orders"]].mean().reset_index().nlargest(top_n_question2, selected_metric)

    # Create 2 columns for layout
    col1, col2 = st.columns(2)

    # Display the top N organizations in a table
    with col1:
        st.header(f'Top {top_n_question2} Organizations (Daily Average)')
        st.table(top_n_df)

    # Create a bar chart for the selected metric
    with col2:
        st.header(f'Bar Chart for {selected_metric} (Mean)')
        top_n_df.set_index('organization_name', inplace=True)
        st.bar_chart(top_n_df[selected_metric])

    #Graphs for Question 3
    selected_column = st.selectbox('Select Column:', question3.columns)
    st.subheader(f'3. What is the average # of {selected_column} per hour of each facility?')

    # Create a bar chart for the selected column
    st.bar_chart(question3[selected_column])

    #Graphs for Question 4
    st.subheader('Average Labor Hour')
    st.line_chart(question4.set_index('Facility')['Labor_hours_actual'])

    # Line graph for 'Labor_Cost'
    st.subheader('Average Labor Cost')
    st.line_chart(question4.set_index('Facility')['Labor_Cost'])

    #Graphs for Question 5
    st.subheader('5. What is the average order per labor hour (OPLH) of each facility?')
    st.bar_chart(question5.set_index('Facility')['average_order_per_labour_hour'])

    #Graphs for Question 6
    st.subheader('6. What is the average processing revenue of each facility?')   
    st.bar_chart(question6.set_index('Facility')['average_processing_revenue_per_facility'])

    #Graphs for Question 7
    st.subheader('7.1 At which facilities are we earning vs losing money considering labor costs and processing revenue?')
    st.dataframe(question7_final)

    st.subheader('7.2 And which days are not correctly staffed (understaffed or overstaffed)?')
    st.dataframe(staffed_result)

    #Graphs for Question 8
    st.subheader('How much incremental income can we earn by optimizing for labor scheduling?')
    st.dataframe(question8)
    st.subheader(f'Incremental Income: {incremental_income}')

# Run the Streamlit app
app()
