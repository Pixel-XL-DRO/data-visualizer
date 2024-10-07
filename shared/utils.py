import streamlit as st
import altair as alt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def create_chart_new(data, x_axis_type, x_axis_label, points_y, line_y, y_axis_label, colorBy, lineStrokeWidth, line_label):
  fig = None

  if points_y and line_y:
    fig = px.line(
      data,
      x=x_axis_type,
      y=line_y,
      color=colorBy if colorBy in data.columns else None,
      title=y_axis_label
    )
    fig.add_traces(px.scatter(
      data,
      x=x_axis_type,
      y=points_y,
      color=colorBy if colorBy in data.columns else None,
      title=y_axis_label
    ).data)


  elif points_y:
    fig = px.scatter(
      data,
      x=x_axis_type,
      y=points_y,
      color=colorBy if colorBy in data.columns else None,
      title=y_axis_label
    )

  elif line_y:
    fig = px.line(
      data,
      x=x_axis_type,
      y=line_y,
      color=colorBy if colorBy in data.columns else None,
      title=y_axis_label
    )

  if not colorBy:
    fig.update_traces(line=dict(width=lineStrokeWidth, color="royalblue"), selector=dict(mode='lines'), hovertemplate='<b>' + line_label + '</b>: %{y}')
  else:
    fig.update_traces(line=dict(width=lineStrokeWidth), selector=dict(mode='lines'), hovertemplate='<b>' + line_label + '</b>: %{y}')

  if not colorBy:
    fig.update_traces(marker=dict(color="orangered"), selector=dict(mode='markers'), hovertemplate='<b>' + y_axis_label + '</b>: %{y}')
  else:
    fig.update_traces(selector=dict(mode='markers'), hovertemplate='<b>' + y_axis_label + '</b>: %{y}')

  fig.update_layout(
    hovermode='x unified',
    dragmode='pan',
    xaxis_title=x_axis_label,
    yaxis_title=y_axis_label,
    xaxis_zeroline=False,
    yaxis=dict(
      spikecolor="red", spikethickness=0.7, spikedash='solid', spikemode='across', spikesnap="cursor"
    ),
    xaxis=dict(
      spikecolor="red", spikethickness=0.7, spikedash='solid', spikemode='across'
    )
  )

  return fig

def create_chart(data, x_axis_type, x_axis_label, points_y, line_y, y_axis_label, colorBy, lineStrokeWidth, tickCount):
    base = alt.Chart(data).encode(
      x=alt.X(x_axis_type, title=x_axis_label, axis=alt.Axis(tickCount=tickCount)),
    ).interactive()

    points = None
    line = None

    if points_y:
      points = base.mark_point().encode(
        y=alt.Y(points_y, title=y_axis_label),
        tooltip=[x_axis_type, points_y],
        color=alt.Color(colorBy, scale=alt.Scale(scheme='dark2')) if colorBy else alt.value('red')
      )

    if line_y:
      line = base.mark_line().encode(
        y=alt.Y(line_y, title=y_axis_label),
        tooltip=[x_axis_type, line_y],
        strokeWidth=alt.value(lineStrokeWidth),
        color=alt.Color(colorBy, scale=alt.Scale(scheme='dark2')) if colorBy else alt.value('blue')
      )

    if points:
       if line:
          return line + points
       return points
    return line

def create_bar_chart(data, x_axis_type, x_axis_label, y_value, y_axis_label, colorBy, currentValue=None):
    base = alt.Chart(data).encode(
      x=alt.X(x_axis_type, title=x_axis_label),
    ).interactive()

    bar = base.mark_bar(size=10).encode(
      y=alt.Y(y_value, title=y_axis_label),
      tooltip=[x_axis_type, y_value],
      color=alt.condition(
        alt.datum[x_axis_type] == currentValue,
        alt.value('orange'),
        alt.value('blue')
      )
    )

    return bar

def make_sure_only_one_toggle_is_on(toggles, key):
  if st.session_state[key]:
    for toggle in toggles:
      if toggle != key:
        st.session_state[toggle] = False


def chain_toggle_off(key_to_check, key_to_toggle):
  if not st.session_state[key_to_check]:
    st.session_state[key_to_toggle] = False

def chain_toggle_on(key_to_check, key_to_toggle):
  if st.session_state[key_to_check]:
    st.session_state[key_to_toggle] = True


def get_month_days_count(year, month):
  return 31 if month in [1, 3, 5, 7, 8, 10, 12] else 30 if month in [4, 6, 9, 11] else 28 if month == 2 and year % 4 == 0 else 29

def get_year_days_count(year):
  return 366 if year % 4 == 0 else 365
