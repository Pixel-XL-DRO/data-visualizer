import utils
import streamlit as st
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import altair as alt

USER_TZ = ZoneInfo("Europe/Warsaw")

visit_name_map = {
    "Integracja firmowa": "Integracje",
    "Wycieczki szkolne": "Szkoły",
    "Wycieczki szkolne / półkolonie": "Szkoły",
}


def get_data(utc_start, utc_end, use_start_date):

    safi_auth_token = st.secrets["safi"].get("auth_token")

    headers = {"Authorization": f"Bearer {safi_auth_token}"}

    params = {"use_start_date": use_start_date, "from_dt": utc_start, "to_dt": utc_end}

    res = requests.get(
        f"https://safi-api.pixel-xl.tech:9999/api/get_bok_income_reservations",
        headers=headers,
        params=params,
    )

    data = res.json()
    res.raise_for_status()

    return data


def parse_data(reservations):

    count_by_role = {
        "Online": 0,
        "Host": 0,
        "CC": 0,
        "By_visit": {},
        "By_location": {},
    }

    report = {
        "Sumaryczne": {},
    }

    for res in reservations:

        role_name = res["role_name"]
        user_name = res["user_name"]

        if user_name and "Host" in user_name:
            user_name = "Host"

        location_name = res["location_name"]
        visit_name = res["visit_name"]

        if visit_name in visit_name_map:
            visit_name = visit_name_map[visit_name]

        brutto = float(res["brutto"])
        netto = float(res["netto"])

        role_key = (
            "Online" if not role_name else ("Host" if role_name == "Worker" else "CC")
        )
        count_by_role[role_key] += 1

        if visit_name not in count_by_role["By_visit"]:
            count_by_role["By_visit"][visit_name] = {"Online": 0, "Host": 0, "CC": 0}
        count_by_role["By_visit"][visit_name][role_key] += 1

        if location_name not in count_by_role["By_location"]:
            count_by_role["By_location"][location_name] = {
                "Online": 0,
                "Host": 0,
                "CC": 0,
            }
        count_by_role["By_location"][location_name][role_key] += 1

        if role_key == "Online":
            continue

        location_brutto_key = f"{location_name}-Brutto"
        location_netto_key = f"{location_name}-Netto"
        location_count_key = f"{location_name}-Liczba"

        def init_entry():
            return {
                "Suma brutto": 0,
                "Suma netto": 0,
                "Liczba": 0,
            }

        def apply_to_entry(e):
            e["Suma brutto"] += brutto
            e["Suma netto"] += netto
            e["Liczba"] += 1
            e[location_brutto_key] = e.get(location_brutto_key, 0) + brutto
            e[location_netto_key] = e.get(location_netto_key, 0) + netto
            e[location_count_key] = e.get(location_count_key, 0) + 1

        if user_name not in report["Sumaryczne"]:
            report["Sumaryczne"][user_name] = init_entry()
        apply_to_entry(report["Sumaryczne"][user_name])

        if visit_name not in report:
            report[visit_name] = {}
        if user_name not in report[visit_name]:
            report[visit_name][user_name] = init_entry()
        apply_to_entry(report[visit_name][user_name])

    return report, count_by_role


@st.fragment
def display_charts(parsed_data, count_by_role):

    all_locations = sorted(count_by_role["By_location"].keys())
    selected_locations = st.multiselect(
        "Filtruj lokacje",
        all_locations,
        default=all_locations,
        key="selected_locations",
    )
    if not selected_locations:
        st.warning("Wybierz co najmniej jedną lokację!")
        return

    filtered_parsed = {}
    for visit_or_sum, users in parsed_data.items():
        filtered_users = {}
        for user, entry in users.items():
            new_entry = {"Suma brutto": 0, "Suma netto": 0, "Liczba": 0}
            for loc in selected_locations:
                b_key = f"{loc}-Brutto"
                n_key = f"{loc}-Netto"
                c_key = f"{loc}-Liczba"
                new_entry["Suma brutto"] += entry.get(b_key, 0)
                new_entry["Suma netto"] += entry.get(n_key, 0)
                new_entry["Liczba"] += entry.get(c_key, 0)
                new_entry[b_key] = entry.get(b_key, 0)
                new_entry[n_key] = entry.get(n_key, 0)
                new_entry[c_key] = entry.get(c_key, 0)
            if new_entry["Liczba"] > 0:
                filtered_users[user] = new_entry
        if filtered_users:
            filtered_parsed[visit_or_sum] = filtered_users

    filtered_by_location = {
        loc: count_by_role["By_location"][loc]
        for loc in selected_locations
        if loc in count_by_role["By_location"]
    }
    filtered_by_visit = {
        visit_name: role_counts
        for visit_name, role_counts in count_by_role["By_visit"].items()
        if visit_name in (filtered_parsed.keys() - {"Sumaryczne"})
    }

    summary = filtered_parsed.get("Sumaryczne", {})
    df = pd.DataFrame(
        [{"user": user, "Liczba": entry["Liczba"]} for user, entry in summary.items()]
    )
    if df.empty:
        st.warning("Brak danych dla wybranych lokacji.")
        return

    df["Procent"] = (df["Liczba"] / df["Liczba"].sum() * 100).round(1)

    bar = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("Liczba:Q", title="Liczba"),
            y=alt.Y("user:N", sort="-x", title="Twórca"),
            color=alt.Color("user:N", legend=None),
            tooltip=[
                alt.Tooltip("user:N", title="Twórca"),
                "Liczba:Q",
                alt.Tooltip("Procent:Q", format=".1f", title="Procent %"),
            ],
        )
    )
    labels = (
        alt.Chart(df)
        .mark_text(align="left", dx=4, size=12)
        .encode(
            x=alt.X("Liczba:Q"),
            y=alt.Y("user:N", sort="-x"),
            text=alt.Text("Procent:Q", format=".1f"),
        )
    )
    st.altair_chart(
        (bar + labels).properties(
            width=700, height=max(100, len(df) * 40), title="Rezerwacje wg twórcy"
        ),
        use_container_width=True,
    )

    for label, breakdown in [
        ("Lokacja", filtered_by_location),
        ("Wizyta", filtered_by_visit),
    ]:
        if not breakdown:
            st.info(f"Brak danych dla grupowania: {label}")
            continue

        role_records = [
            {"Grupa": group_name, "role": role, "Liczba": count}
            for group_name, role_counts in breakdown.items()
            for role, count in role_counts.items()
        ]
        role_df = pd.DataFrame(role_records)

        chart = (
            alt.Chart(role_df)
            .transform_joinaggregate(GrupaTotal="sum(Liczba)", groupby=["Grupa"])
            .transform_calculate(Procent="datum.Liczba / datum.GrupaTotal * 100")
            .mark_bar()
            .encode(
                x=alt.X(
                    "Procent:Q",
                    stack="normalize",
                    axis=alt.Axis(format="%", title="Udział"),
                ),
                y=alt.Y("Grupa:N", title=label),
                color=alt.Color("role:N", legend=alt.Legend(title="Rola")),
                tooltip=[
                    alt.Tooltip("Grupa:N", title=label),
                    alt.Tooltip("role:N", title="Rola"),
                    alt.Tooltip("Liczba:Q", title="Liczba"),
                    alt.Tooltip("Procent:Q", title="% w grupie", format=".1f"),
                ],
            )
        )

        text = (
            alt.Chart(role_df)
            .transform_joinaggregate(GrupaTotal="sum(Liczba)", groupby=["Grupa"])
            .transform_calculate(Procent="datum.Liczba / datum.GrupaTotal * 100")
            .transform_filter(alt.datum.Liczba / alt.datum.GrupaTotal > 0.05)
            .mark_text(
                color="white",
                fontWeight="bold",
                fontSize=11,
                align="center",
                baseline="middle",
            )
            .encode(
                x=alt.X("Procent:Q", stack="normalize"),
                y=alt.Y("Grupa:N"),
                detail="role:N",
                text=alt.Text("Procent:Q", format=".1f"),
            )
        )

        st.altair_chart(
            (chart + text)
            .properties(
                width=700,
                height=max(200, len(role_df["Grupa"].unique()) * 40),
                title=f"Rezerwacje wg roli i {label.lower()}",
            )
            .configure_view(stroke=None),
            use_container_width=True,
        )

    visit_location_records = []
    for visit_name, users in filtered_parsed.items():
        if visit_name == "Sumaryczne":
            continue
        for loc in selected_locations:
            loc_count = sum(entry.get(f"{loc}-Liczba", 0) for entry in users.values())
            if loc_count > 0:
                visit_location_records.append(
                    {
                        "Lokacja": loc,
                        "Wizyta": visit_name,
                        "Liczba": loc_count,
                    }
                )

    vl_df = pd.DataFrame(visit_location_records)

    if not vl_df.empty:
        chart = (
            alt.Chart(vl_df)
            .transform_joinaggregate(LocTotal="sum(Liczba)", groupby=["Lokacja"])
            .transform_calculate(Procent="datum.Liczba / datum.LocTotal * 100")
            .mark_bar()
            .encode(
                x=alt.X(
                    "Procent:Q",
                    stack="normalize",
                    axis=alt.Axis(format="%", title="Udział"),
                ),
                y=alt.Y("Lokacja:N", title="Lokacja"),
                color=alt.Color("Wizyta:N", legend=alt.Legend(title="Typ wizyty")),
                tooltip=[
                    alt.Tooltip("Lokacja:N", title="Lokacja"),
                    alt.Tooltip("Wizyta:N", title="Typ wizyty"),
                    alt.Tooltip("Liczba:Q", title="Liczba"),
                    alt.Tooltip("Procent:Q", title="% w lokacji", format=".1f"),
                ],
            )
        )

        text = (
            alt.Chart(vl_df)
            .transform_joinaggregate(LocTotal="sum(Liczba)", groupby=["Lokacja"])
            .transform_calculate(Procent="datum.Liczba / datum.LocTotal * 100")
            .transform_filter(alt.datum.Liczba / alt.datum.LocTotal > 0.05)
            .mark_text(
                color="white",
                fontWeight="bold",
                fontSize=11,
                align="center",
                baseline="middle",
            )
            .encode(
                x=alt.X("Procent:Q", stack="normalize"),
                y=alt.Y("Lokacja:N"),
                detail="Wizyta:N",
                text=alt.Text("Procent:Q", format=".1f"),
            )
        )

        st.altair_chart(
            (chart + text)
            .properties(
                width=700,
                height=max(200, len(vl_df["Lokacja"].unique()) * 40),
                title="Udział wizyt wg lokacji",
            )
            .configure_view(stroke=None),
            use_container_width=True,
        )

    creator_visit_records = []
    for visit_name, users in filtered_parsed.items():
        if visit_name == "Sumaryczne":
            continue
        for user, entry in users.items():
            count = entry.get("Liczba", 0)
            if count > 0:
                creator_visit_records.append(
                    {
                        "Twórca": user,
                        "Wizyta": visit_name,
                        "Liczba": count,
                    }
                )

    cv_df = pd.DataFrame(creator_visit_records)

    if not cv_df.empty:
        chart = (
            alt.Chart(cv_df)
            .transform_joinaggregate(TwórcaTotal="sum(Liczba)", groupby=["Twórca"])
            .transform_calculate(Procent="datum.Liczba / datum.TwórcaTotal * 100")
            .mark_bar()
            .encode(
                x=alt.X(
                    "Procent:Q",
                    stack="normalize",
                    axis=alt.Axis(format="%", title="Udział"),
                ),
                y=alt.Y("Twórca:N", title="Twórca"),
                color=alt.Color("Wizyta:N", legend=alt.Legend(title="Typ wizyty")),
                tooltip=[
                    alt.Tooltip("Twórca:N", title="Twórca"),
                    alt.Tooltip("Wizyta:N", title="Typ wizyty"),
                    alt.Tooltip("Liczba:Q", title="Liczba"),
                    alt.Tooltip("Procent:Q", title="% u twórcy", format=".1f"),
                ],
            )
        )

        text = (
            alt.Chart(cv_df)
            .transform_joinaggregate(TwórcaTotal="sum(Liczba)", groupby=["Twórca"])
            .transform_calculate(Procent="datum.Liczba / datum.TwórcaTotal * 100")
            .transform_filter(alt.datum.Liczba / alt.datum.TwórcaTotal > 0.05)
            .mark_text(
                color="white",
                fontWeight="bold",
                fontSize=11,
                align="center",
                baseline="middle",
            )
            .encode(
                x=alt.X("Procent:Q", stack="normalize"),
                y=alt.Y("Twórca:N"),
                detail="Wizyta:N",
                text=alt.Text("Procent:Q", format=".1f"),
            )
        )

        st.altair_chart(
            (chart + text)
            .properties(
                width=700,
                height=max(200, len(cv_df["Twórca"].unique()) * 40),
                title="Udział typów wizyt wg twórcy",
            )
            .configure_view(stroke=None),
            use_container_width=True,
        )
        heatmap_records = []
        for user, entry in summary.items():
            for loc in selected_locations:
                count = entry.get(f"{loc}-Liczba", 0)
                if count > 0:
                    heatmap_records.append(
                        {"Twórca": user, "Lokacja": loc, "Liczba": count}
                    )

        heatmap_df = pd.DataFrame(heatmap_records)
        if not heatmap_df.empty:
            selection = alt.selection_point(fields=["Twórca"], bind="legend")

            stacked_bar = (
                alt.Chart(heatmap_df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "Liczba:Q",
                        stack="normalize",
                        axis=alt.Axis(format="%", title="Udział"),
                    ),
                    y=alt.Y("Lokacja:N", title="Lokacja"),
                    color=alt.Color(
                        "Twórca:N",
                        legend=alt.Legend(title="Twórca", orient="bottom", columns=4),
                    ),
                    opacity=alt.condition(selection, alt.value(1.0), alt.value(0.15)),
                    tooltip=[
                        alt.Tooltip("Twórca:N", title="Twórca"),
                        alt.Tooltip("Lokacja:N", title="Lokacja"),
                        alt.Tooltip("Liczba:Q", title="Liczba"),
                        alt.Tooltip("Procent:Q", title="%", format=".1f"),
                    ],
                )
                .transform_joinaggregate(
                    LokacjaTotal="sum(Liczba)", groupby=["Lokacja"]
                )
                .transform_calculate(Procent="datum.Liczba / datum.LokacjaTotal * 100")
                .add_params(selection)
                .properties(
                    width=700,
                    height=max(300, len(heatmap_df["Lokacja"].unique()) * 70),
                    title="Liczba rezerwacji wg twórcy i lokacji",
                )
            )

            stacked_bar


def view():

    date_col1, date_col2 = st.columns(2)

    now = datetime.now()
    with date_col1:
        start_date = st.date_input(
            "Podaj date poczatku",
            now - timedelta(days=7),
            key="start_date",
            max_value=now - timedelta(days=1),
        )

    with date_col2:
        end_date = st.date_input("Podaj date końca", now, key="end_date")

    dt_start_date = datetime.combine(start_date, datetime.min.time(), tzinfo=USER_TZ)
    dt_end_date = datetime.combine(end_date, datetime.min.time(), tzinfo=USER_TZ)

    utc_start = (
        dt_start_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    )

    dt_end = datetime.combine(
        dt_end_date + timedelta(days=1), datetime.min.time(), tzinfo=USER_TZ
    )

    utc_end = dt_end.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    date_type = st.selectbox(
        "Wybierz rodzaj daty", ["Data stworzenia", "Data rozpoczecia"], key="date_type"
    )
    use_start_date = True if date_type == "Data rozpoczecia" else False

    if st.button("Generuj raport"):
        with st.spinner("Ładowanie danych...", show_time=True):
            data = get_data(utc_start, utc_end, use_start_date)
            parsed_data, count_by_role = parse_data(data)

            if not parsed_data:
                return
            st.info("Dane są gotowe do pobrania!")
            utils.download_button(
                parsed_data,
                f"Dane BOK w przedziale {start_date}-{end_date}",
                label="Pobierz raport .xlxs",
            )

            display_charts(parsed_data, count_by_role)


view()
