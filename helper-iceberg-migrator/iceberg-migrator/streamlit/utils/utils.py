import streamlit as st
import base64
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def render_image(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=585)


def render_image_summary(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=125)


def render_image_menu(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=150)
    
def render_image_menu2(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=165)


def render_image_true_size(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string)


def format_as_percentage(value):
    if value is not None:
        return '{:.2%}'.format(value)
    else:
        return None


def format_with_commas(value):
	if value is not None:
		return '{:,}'.format(value)
	else:
		return None


def create_styled_table(header_values, cell_values, column_widths=None, height=150, cell_alignments=None, width=100):

    if cell_alignments is None:
        # Default to center alignment if cell_alignments is not provided
        cell_alignments = ['center'] 
	
    if column_widths is None:
        # Use the default_width for all columns if column_widths is not provided
        num_columns = len(header_values)
        column_widths = [1] * num_columns

   # total_width = sum(column_widths)
    
    table_chart = go.Figure(data=[go.Table(
        columnwidth=column_widths,            
        
        header=dict(values=header_values,
                    fill_color='#E6EDD6',
                    line_color='white',
                    font=dict(color='black', size=14)),
        cells=dict(values=cell_values,
                   fill_color='#F0F2F6',
                   line_color='white',
                   font=dict(color='black', size=12),
                   height=30,  # Set the cell height
                   align=cell_alignments # Align the cells based on the specified alignments or defaults to center
                ))
    ])
    table_chart.update_layout(width=width, height=height)  # Set the total table width
    table_chart.update_layout(margin=dict(t=0, r=0, b=0, l=0))
    return table_chart

def create_table_no_header(header_values, cell_values, height=100, width=500):
    table = go.Figure(data=[go.Table( 
        columnwidth=[0.5, 0.5],
        header=dict(values=header_values,
                    fill_color='white',
                    line_color='white', 
                    font=dict(color='white')), 
        cells=dict(values=cell_values, 
                    fill_color='white',
                    line_color='white',
                    font=dict(color='black', size=12))) 
        ])
    table.update_layout(height=height, width=width, margin=dict(t=5, r=5, b=5, l=5))

    return table

def create_custom_pie_chart(labels, values, width=350, height=350):
    colors = ['#435F4F', '#85AD98', '#77933C', '#E6E9F4']

    chart_pie = px.pie(names=labels, values=values, hole=0.5, color_discrete_sequence=colors)
    chart_pie.update_traces(textinfo='percent+label', textfont=dict(color='black', size=10), textposition='inside', texttemplate='%{percent:.2%}',  hoverinfo='skip',hovertemplate='%{label}')
    chart_pie.update_layout(legend_font=dict(size=10), legend=dict(x=-0.2, y=-0.2, orientation='h'))
    chart_pie.update_layout(
        width=width,  # Default width, can be changed
        height=height,  # Default height, can be changed
    )
    chart_pie.update_layout(margin=dict(t=10, r=10, b=10, l=10))
    
    return chart_pie


def paginate_data(df):
	#st.divider()
			
	pagination = st.empty()
	batch_size = 20  # Set the number of items per page
 
	bottom_menu = st.columns((4, 1, 1))
	with bottom_menu[2]:
		total_pages = (
			int(len(df) / batch_size) if int(len(df) / batch_size) > 0 else 1
		)
		current_page = st.number_input(
			"Page", min_value=1, max_value=total_pages, step=1
		)
	with bottom_menu[0]:
		st.markdown(f"Page **{current_page}** of **{total_pages}** ")

	pages = split_frame(df, batch_size)
	pagination.dataframe(data=pages[current_page - 1], use_container_width=True, hide_index=True)

	#st.divider()
 
 
#@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
	df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
	return df


def dataframe_with_selections(df, pre_sel_clmns):
    df_with_selections = df.copy()
    
    sel_clmn = False
    if pre_sel_clmns:
        sel_clmn = any(clmn == df_with_selections.iloc[0,0] for clmn in pre_sel_clmns)

    df_with_selections.insert(0, "Select", sel_clmn)

    # Get dataframe row-selections from user with st.data_editor
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
        use_container_width=True,
    )

    # Filter the dataframe using the temporary column, then drop the column
    selected_rows = edited_df[edited_df.Select]
    return selected_rows.drop('Select', axis=1)	
	

def col_name_check(col_name):
    special_chars = " !\"#$%&'()*+,./:;<=>?@[\]^`{|}~" #allow dash (minus) and underscore in collection name
    if any(c in special_chars for c in col_name):
        return True
    else:
        return False
    
@st.cache_data(show_spinner=False)
def query_cache(_session, input_stmt):
	df = pd.DataFrame(_session.sql(input_stmt).collect())
	return df