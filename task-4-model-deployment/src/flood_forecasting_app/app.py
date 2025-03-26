import streamlit as st


pages = {"Menu": [
        st.Page("main.py", title="Main"),
        st.Page("about.py", title="About"),
        ]}

pg = st.navigation(pages)
pg.run()