import pandas as pd
import pymysql as db
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go



user="phonepay"
passcode="PhonePay@14"
host_name="localhost"
db_name="pp"

db_connection = db.connect(
                user=user,
                database=db_name,
                password=passcode,
                host=host_name)

my_cursor=db_connection.cursor()


sql_1="""select * from Agg_Trans"""

my_cursor.execute(sql_1)
Agg_Trans=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Agg_Trans=pd.DataFrame(Agg_Trans, columns=columns)

sql_2="""select * from Agg_Insu"""

my_cursor.execute(sql_2)
Agg_Insu=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Agg_Insu=pd.DataFrame(Agg_Insu, columns=columns)

sql_3="""select * from Agg_User"""


my_cursor.execute(sql_3)
Agg_User=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Agg_User=pd.DataFrame(Agg_User, columns=columns)

# 1. Decoding Transaction Dynamics on PhonePe

sql_4="""SELECT 
    State,
    Year,
    Quater,
    Transacion_type,
    SUM(Transacion_Count) AS Total_Transactions,
    SUM(Transacion_Amount) AS Total_Amount
    FROM Agg_Trans
    GROUP BY 
        State, Year, Quater, Transacion_type
    ORDER BY 
        State, Year, Quater"""

my_cursor.execute(sql_4)
Decoding_Transaction=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Decoding_Transaction=pd.DataFrame(Decoding_Transaction, columns=columns)



# 2. Device Dominance and User Engagement Analysis

sql_4="""SELECT 
    State,
    User_brand,
    Year,
    Quater,
    SUM(registeredUsers) AS Total_Registered_Users,
    SUM(appOpens) AS Total_App_Opens,
    CASE 
        WHEN SUM(registeredUsers) = 0 THEN 0
        ELSE ROUND(SUM(appOpens) * 1.0 / SUM(registeredUsers), 2)
    END AS Engagement_Rate
FROM Agg_User
GROUP BY 
    State, User_brand, Year, Quater
ORDER BY 
    State, Engagement_Rate DESC"""
my_cursor.execute(sql_4)
User_Engagement_Analysis=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
User_Engagement_Analysis= pd.DataFrame(User_Engagement_Analysis,columns=columns)

# 3.Transaction Analysis for Market Expansion

sql_5="""SELECT 
    State,
    Year,
    Quater,
    district_name,
    SUM(district_Transacion_Count) AS Total_Transactions,
    SUM(district_Transacion_Amount) AS Total_Amount
FROM top_tans_district
GROUP BY 
    State, Year, Quater, district_name
ORDER BY 
    State, Year, Quater"""
my_cursor.execute(sql_5)
Transaction_Analysis=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Transaction_Analysis= pd.DataFrame(Transaction_Analysis,columns=columns)

# 4.Insurance Transactions Analysis
# state Insurance
sql_6="""SELECT 
   State,
    Year,
    Quater,
    SUM(Insurance_Transacion_count) AS Total_Transacions,
    SUM(Insurance_Transacion_amount) AS Total_Transacion_Amount
FROM Agg_Insu
GROUP BY State,Year,Quater
ORDER BY Total_Transacion_Amount DESC"""

my_cursor.execute(sql_6)
State_Insurance_Analysis=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
State_Insurance_Analysis= pd.DataFrame(State_Insurance_Analysis,columns=columns)

#  District Insurance

sql_7="""SELECT 
    State,
    district_name,
    Year,
    Quater,
    SUM(Insurance_count) AS Total_Transactions,
    SUM(Insurance_amount) AS Total_Transaction_Amount
FROM Top_district_insu
GROUP BY State, district_name,Year,Quater
ORDER BY Total_Transaction_Amount DESC"""

my_cursor.execute(sql_7)
District_Insurance_Analysis=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
District_Insurance_Analysis= pd.DataFrame(District_Insurance_Analysis,columns=columns)

# postal Insurance
sql_7="""SELECT 
    State,
    postal_name,
    Year,
    Quater,
    SUM(Insurance_count) AS Total_Transactions,
    SUM(Insurance_amount) AS Total_Transaction_Amount
FROM top_postal_insu
GROUP BY State, postal_name,Year,Quater
ORDER BY Total_Transaction_Amount DESC"""

my_cursor.execute(sql_7)
Postal_Insurance_Analysis=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Postal_Insurance_Analysis= pd.DataFrame(Postal_Insurance_Analysis,columns=columns)

#  5. User Registration Analysis
# Top States by Registered Users

sql_8="""SELECT 
    State,
    Year,
    Quater,
    SUM(registeredUsers) AS Total_Registered_Users
FROM Agg_User
GROUP BY State,Year,Quater
ORDER BY Total_Registered_Users DESC"""

my_cursor.execute(sql_8)
Top_state_User=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Top_state_User= pd.DataFrame(Top_state_User,columns=columns)

#   Top Districts by Registered Users

sql_9="""SELECT 
    State,
    district_name,
    Year,
    Quater,
    SUM(registeredUsers) AS Total_Registered_Users
FROM Top_district_user
GROUP BY State, district_name,Year,Quater
ORDER BY Total_Registered_Users DESC"""


my_cursor.execute(sql_9)
Top_district_user=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Top_district_user= pd.DataFrame(Top_district_user,columns=columns)

#  Top Pin Codes by Registered Users

sql_9="""SELECT 
    State,
    postal_name,
    Year,
    Quater,
    SUM(registeredUsers) AS Total_Registered_Users
FROM Top_postal_user
GROUP BY State, postal_name,Year,Quater
ORDER BY Total_Registered_Users DESC"""


my_cursor.execute(sql_9)
Top_postal_user=my_cursor.fetchall()
columns=[col[0] for col in my_cursor.description]
Top_postal_user= pd.DataFrame(Top_postal_user,columns=columns)


# functions for state rearrange
def state(table):
    state_mapping = {
    "Andaman-&-Nicobar-Islands": "Andaman & Nicobar",
    "Andhra-Pradesh": "Andhra Pradesh",
    "Arunachal-Pradesh": "Arunachal Pradesh",
    "Dadra-&-Nagar-Haveli-&-Daman-&-Diu" : "Dadra and Nagar Haveli and Daman and Diu",
    "Himachal-Pradesh": "Himachal Pradesh",
    "Jammu-&-Kashmir": "Jammu & Kashmir",
    "Madhya-Pradesh" : "Madhya Pradesh",
    "Tamil-Nadu": "Tamil Nadu",
    "Uttar-Pradesh" : "Uttar Pradesh",
    "West-Bengal" : "West Bengal",
    }
    table['State'] = table['State'].replace(state_mapping)
    return table
    

Agg_Trans=state(Agg_Trans)
Agg_Insu=state(Agg_Insu)
Agg_User=state(Agg_User)
Decoding_Transaction= state(Decoding_Transaction)
User_Engagement_Analysis= state(User_Engagement_Analysis)
Transaction_Analysis= state(Transaction_Analysis)
State_Insurance_Analysis = state(State_Insurance_Analysis)
Top_state_User= state(Top_state_User)
District_Insurance_Analysis= state(District_Insurance_Analysis)
Postal_Insurance_Analysis = state(Postal_Insurance_Analysis)
Top_district_user = state(Top_district_user)
Top_postal_user= state(Top_postal_user)

st.title("Phone Pay")

r = st.sidebar.radio("Navigation",["Home","Business Case Study"])

if r == "Home":
    col1,col2,col3= st.columns(3,gap = "small")
    Selected_type=col1.selectbox("Select Type",["Transcation","Insurance","User"])
    if Selected_type == "Transcation":
        selected_year = col2.selectbox("Select Year", Agg_Trans["Year"].unique())
        selected_quarter = col3.selectbox("Select Quarter", Agg_Trans["Quater"].unique())
        filtered_df = Agg_Trans[
        (Agg_Trans["Year"] == selected_year) & (Agg_Trans["Quater"] == selected_quarter)
        ]
        fig = px.choropleth(
        filtered_df,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="State",
        color="Transacion_count",  
        color_continuous_scale="Reds",
        title=f"Transactions in {selected_year} Q{selected_quarter}",
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig, use_container_width=True)

    elif Selected_type == "Insurance":
        selected_year = col2.selectbox("Select Year", Agg_Insu["Year"].unique())
        selected_quarter = col3.selectbox("Select Quarter", Agg_Insu["Quater"].unique())
        filtered_df_insu = Agg_Insu[
        (Agg_Insu["Year"] == selected_year) & (Agg_Insu["Quater"] == selected_quarter)
        ]
        fig1 = px.choropleth(
        filtered_df_insu,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="State",
        color="Insurance_Transacion_count",  # <- if different column, update accordingly
        color_continuous_scale="Reds",
        title=f"Insurance in {selected_year} Q{selected_quarter}",
        )
        fig1.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig1, use_container_width=True)
    elif Selected_type == "User":
        selected_year = col2.selectbox("select Year", Agg_User["Year"].unique())
        selected_quarter= col3.selectbox("select Quater", Agg_User["Quater"].unique())
        filtered_df_user= Agg_User[
        (Agg_User["Year"]== selected_year)  & (Agg_User["Quater"]== selected_quarter)
        ]
        fig2 = px.choropleth(
        filtered_df_user,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="State",
        color="User_count",  # <- if different column, update accordingly
        color_continuous_scale="Reds",
        title=f"User Count in {selected_year} Q{selected_quarter}",
        )
        fig2.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.header("PhonePay Analysis")
    st.subheader("Business Case studies")
    selected_question=st.selectbox("select Business case Study",["1. Decoding Transaction Dynamics on PhonePe",
    "2.Device Dominance and User Engagement Analysis","3.Transaction Analysis for Market Expansion","4.Insurance Transactions Analysis",
    "5.User Registration Analysis"])
    if selected_question == "1. Decoding Transaction Dynamics on PhonePe":
        col1, col2 = st.columns(2,gap="small")
        selected_year=col1.selectbox("select Year",Decoding_Transaction["Year"].unique())
        selected_quarter=col2.selectbox("select Quater",Decoding_Transaction["Quater"].unique())
        filtered_df1 = Decoding_Transaction[
        (Decoding_Transaction["Year"] == selected_year) & (Decoding_Transaction["Quater"] == selected_quarter)
        ]
        fig3 = px.choropleth(
        filtered_df1,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="State",
        color="Total_Transactions",
        color_continuous_scale="Reds",
        )
        fig3.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig3, use_container_width=True)
        pie_data=filtered_df1["Transacion_type"].value_counts()
        pie_fig = go.Figure(data=[go.Pie(labels=pie_data.index, values=pie_data.values)])
        st.plotly_chart(pie_fig)
    elif selected_question == "2.Device Dominance and User Engagement Analysis":
        col1, col2 = st.columns(2,gap="small")
        selected_year=col1.selectbox("select Year",User_Engagement_Analysis["Year"].unique())
        selected_quarter=col2.selectbox("select Quater",User_Engagement_Analysis["Quater"].unique())
        filtered_df2 = User_Engagement_Analysis[
        (User_Engagement_Analysis["Year"] == selected_year) & (User_Engagement_Analysis["Quater"] == selected_quarter)
        ]
        fig4 = px.choropleth(
        filtered_df2,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey="properties.ST_NM",
        locations="State",
        color="Total_Registered_Users",
        color_continuous_scale="Reds",
        )
        fig4.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig4, use_container_width=True)
        pie_data=filtered_df2["User_brand"].value_counts()
        pie_fig = go.Figure(data=[go.Pie(labels=pie_data.index, values=pie_data.values, title="User Brand year wise")])
        st.plotly_chart(pie_fig)
    elif selected_question=="3.Transaction Analysis for Market Expansion":
        col1,col2=st.columns(2,gap="small")
        selected_year=col1.selectbox("select Year",Transaction_Analysis["Year"].unique())
        selected_quarter=col2.selectbox("select Quater",Transaction_Analysis["Quater"].unique())
        filtered_df3 = Transaction_Analysis[
        (Transaction_Analysis["Year"] == selected_year) & (Transaction_Analysis["Quater"] == selected_quarter)
        ]
        district_data = filtered_df3.groupby("district_name")["Total_Transactions"].sum().reset_index()
        top_districts = district_data.sort_values(by="Total_Transactions", ascending=False).head(20)
        fig_bar1 = px.bar(top_districts, x="Total_Transactions", y="district_name", orientation='h',
        title="Top 20 Districts by Transaction Count")
        fig_bar1.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_bar1)
    elif selected_question== "4.Insurance Transactions Analysis":
        selected_analysis=st.selectbox("Select analysis", ["State Insurance","District Insurance", "Postal Insurance"])
        if selected_analysis=="State Insurance":
            col1,col2=st.columns(2,gap="small")
            selected_year=col1.selectbox("select Year",State_Insurance_Analysis["Year"].unique())
            selected_quarter=col2.selectbox("select Quater",State_Insurance_Analysis["Quater"].unique())
            filtered_df4 = State_Insurance_Analysis[
            (State_Insurance_Analysis["Year"] == selected_year) & (State_Insurance_Analysis["Quater"] == selected_quarter)
            ]
            fig5 = px.choropleth(
            filtered_df4,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="State",
            color="Total_Transacions",
            color_continuous_scale="Reds",
            )
            fig5.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig5, use_container_width=True)
            state_data=filtered_df4.groupby("State")["Total_Transacions"].sum().reset_index()
            top_state_insurance=state_data.sort_values(by="Total_Transacions", ascending = False).head(20)
            fig_bar2 = px.bar(top_state_insurance,x="Total_Transacions", y ="State",orientation='h',
            title="Top 20 State_insurance by Transaction Count")
            fig_bar2.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar2)
        elif selected_analysis=="District Insurance":
            col1,col2,col3=st.columns(3,gap="small")
            selected_year=col1.selectbox("select year",District_Insurance_Analysis["Year"].unique())
            selected_quarter=col2.selectbox("select quater",District_Insurance_Analysis["Quater"].unique())
            selected_state=col3.selectbox("select state",District_Insurance_Analysis["State"].unique())
            filtered_df5=District_Insurance_Analysis[
            (District_Insurance_Analysis["Year"] == selected_year) & (District_Insurance_Analysis["Quater"]== selected_quarter) &
            (District_Insurance_Analysis["State"]== selected_state)
            ]
            dis_data=filtered_df5.groupby("district_name")["Total_Transactions"].sum().reset_index()
            top_distict_insurance=dis_data.sort_values(by="Total_Transactions", ascending = False).head(20)
            fig_bar3 = px.bar(top_distict_insurance,x="Total_Transactions", y ="district_name",orientation='h',
            title="Top 20 District_insurance by Transaction Count")
            fig_bar3.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar3)
        elif selected_analysis==("Postal Insurance"):
            col1,col2,col3=st.columns(3,gap="small")
            selected_year=col1.selectbox("select year",Postal_Insurance_Analysis["Year"].unique())
            selected_quarter=col2.selectbox("select quater",Postal_Insurance_Analysis["Quater"].unique())
            selected_state=col3.selectbox("selected State",Postal_Insurance_Analysis["State"].unique())
            filtered_df6=Postal_Insurance_Analysis[
            (Postal_Insurance_Analysis["Year"] == selected_year) & (Postal_Insurance_Analysis["Quater"]== selected_quarter) &
            (Postal_Insurance_Analysis["State"] == selected_state)
            ]
            postel_data=filtered_df6.groupby("postal_name")["Total_Transactions"].sum().reset_index()
            top_postal_insurance=postel_data.sort_values(by="Total_Transactions", ascending = False).head(20)
            fig_bar4 = px.line(top_postal_insurance,x="Total_Transactions", y ="postal_name",
            title="Top 20 Postal_insurance by Transaction Count")
            fig_bar4.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar4)
    elif selected_question == "5.User Registration Analysis":
        selected_analysis=st.selectbox("Select analysis", ["State Registered Users","District Registered Users", "Postal Registered Users"])
        if selected_analysis=="State Registered Users":
            col1,col2=st.columns(2,gap="small")
            selected_year=col1.selectbox("select Year",Top_state_User["Year"].unique())
            selected_quarter=col2.selectbox("select Quater",Top_state_User["Quater"].unique())
            filtered_df7 = Top_state_User[
            (Top_state_User["Year"] == selected_year) & (Top_state_User["Quater"] == selected_quarter)
            ]
            fig6 = px.choropleth(
            filtered_df7,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey="properties.ST_NM",
            locations="State",
            color="Total_Registered_Users",
            color_continuous_scale="Reds",
            )
            fig6.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig6, use_container_width=True)
            state_user=filtered_df7.groupby("State")["Total_Registered_Users"].sum().reset_index()
            top_state_User=state_user.sort_values(by="Total_Registered_Users", ascending = False).head(20)
            fig_bar5 = px.bar(top_state_User,x="Total_Registered_Users", y ="State",orientation='h',
            title="Top 20 State_User by Total Registered user")
            fig_bar5.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar5)
        elif selected_analysis=="District Registered Users":
            col1,col2,col3=st.columns(3,gap="small")
            selected_year=col1.selectbox("select year",Top_district_user["Year"].unique())
            selected_quarter=col2.selectbox("select quater",Top_district_user["Quater"].unique())
            selected_state=col3.selectbox("select state",Top_district_user["State"].unique())
            filtered_df8=Top_district_user[
            (Top_district_user["Year"] == selected_year) & (Top_district_user["Quater"]== selected_quarter) &
            ( Top_district_user["State"] == selected_state)
            ]
            disctrict_user=filtered_df8.groupby("district_name")["Total_Registered_Users"].sum().reset_index()
            Top_District_users=disctrict_user.sort_values(by="Total_Registered_Users", ascending = False).head(20)
            fig_bar6 = px.bar(Top_District_users,x="Total_Registered_Users", y ="district_name",orientation='h',
            title="Top 20 District_User by Total Registered Users")
            fig_bar6.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar6)
        elif selected_analysis=="Postal Registered Users":
            col1,col2,col3=st.columns(3,gap="small")
            selected_year=col1.selectbox("select year",Top_postal_user["Year"].unique())
            selected_quarter=col2.selectbox("select quater",Top_postal_user["Quater"].unique())
            selected_state=col3.selectbox("select state",Top_postal_user["State"].unique())
            filtered_df9=Top_postal_user[
            (Top_postal_user["Year"] == selected_year) & (Top_postal_user["Quater"]== selected_quarter) &
            (Top_postal_user["State"]== selected_state)
            ]
            Postal_user=filtered_df9.groupby("postal_name")["Total_Registered_Users"].sum().reset_index()
            Top_postal_users=Postal_user.sort_values(by="Total_Registered_Users", ascending = False).head(20)
            fig_bar7 = px.line(Top_postal_users,x="Total_Registered_Users", y ="postal_name",
            title="Top 20 Postal_User by Total Registered Users")
            fig_bar7.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_bar7)
            