import streamlit as st
import altair as alt

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

def create_bar_chart(data, x_axis_type, x_axis_label, y_value, y_axis_label, colorBy):
    base = alt.Chart(data).encode(
      x=alt.X(x_axis_type, title=x_axis_label),
    ).interactive()

    bar = base.mark_bar(size=10).encode(
      y=alt.Y(y_value, title=y_axis_label),
      tooltip=[x_axis_type, y_value],
      color=alt.Color(colorBy, scale=alt.Scale(scheme='dark2')) if colorBy else alt.value('green')
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
