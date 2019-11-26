import argparse
import shutil
import pandas as pd
from paceutils import Utilization

u = Utilization()


def build_csv_date_spread(func, table, facility_col, start_date, end_date):
    """
    Loops through each month between the start and end dates provided and
    creates a dataframe with the funcs result for each month.

    Begins by getting all facilities that appear in the data during
    the time period.

    Args:
        func: python function that create a dataframe where the rows are
            facilities and the column is the indicator values for each facility
            during the month
        table: SQL table in the database that the func pulls data from
        facility_col: column in SQL table that contains information on facilities
        start_date: start date of the time period to create dataframe for
        end_date: end date of the time period to create dataframe for

    Returns:
        DataFrame: contains rows with facilities and the 
            indicator value for each month in the time period.
    """
    if table == "authorizations":
        where_filter = "WHERE service_type IS 'Adult Day Center Attendance'"
    elif table == "inpatient":
        where_filter = "WHERE admission_type = 'Acute Hospital' OR admission_type = 'Psych Unit / Facility'"
    else:
        where_filter = ""

    df = u.dataframe_query(
        f"""SELECT DISTINCT({facility_col}) FROM {table} {where_filter};"""
    )

    for date in pd.date_range(start_date, end_date, freq="M"):
        start_date = pd.to_datetime(f"{date.year}-{date.month}-01").date()
        params = (str(start_date), str(date.date()))

        df = df.merge(func(params), on=facility_col, how="left").fillna(0)

    df.sort_values(by=facility_col, inplace=True)

    return df


def build_pressure_wound_csv(facility_type, start_date, end_date):
    """
    Loops through each month between the start and end dates provided and
    creates a dataframe with the pressure ulcer result for each month.

    Begins by getting all facilities that appear in the wounds data during
    the time period.

    Args:
        facility_type: Determines if the function looks for wounds
            at ALFs or SNFs.
        start: start date of the time period to create dataframe for
        end: end date of the time period to create dataframe for

    Returns:
        DataFrame: contains rows with facilities and the 
            number of pressure wounds for each month in the time period.
    """
    df = u.dataframe_query(
        """SELECT DISTINCT(living_detail) FROM wounds
        WHERE living_situation IS ?""",
        [facility_type],
    )
    df["living_detail"].fillna("Unknown", inplace=True)
    for date in pd.date_range(start_date, end_date, freq="M"):
        start_date = pd.to_datetime(f"{date.year}-{date.month}-01").date()
        params = (str(start_date), str(date.date()))

        df = df.merge(
            pressure_ulcers_at_facility(facility_type, params),
            on="living_detail",
            how="left",
        ).fillna(0)

    df.sort_values(by="living_detail", inplace=True)
    return df


def alf_census_on_date(params):
    """
    Number of ppts at each ALF during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            """SELECT * FROM alfs
                            WHERE (discharge_date >= ?
                            OR discharge_date IS NULL)
                            AND admission_date <= ?;""",
            params,
        )
        .groupby("facility_name")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"census-{params[0][:7]}"})
    )


def alf_to_hosp(month_params):
    """
    Number of ppts that discharge from an ALF to a hospital.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            """SELECT * FROM alfs
            WHERE discharge_date BETWEEN ? AND ?
            AND discharge_type='Hospital/ER';""",
            params=month_params,
        )
        .groupby("facility_name")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"hosp_admits-{month_params[0]}"})
    )


def nf_census_on_date(params):
    """
    Number of ppts at each Nursing Facility during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            """SELECT * FROM inpatient
                            WHERE (discharge_date >= ?
                            OR discharge_date IS NULL)
                            AND admission_date <= ?;""",
            params,
        )
        .groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"census-{params[0][:7]}"})
    )


def nf_to_hosp(month_params):
    """
    Number of ppts that discharge from an nursing facility to a hospital.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            """SELECT * FROM nursing_home
            WHERE discharge_date BETWEEN ? AND ?
            AND discharge_disposition IS 'Acute care hospital or psychiatric facility';""",
            params=month_params,
        )
        .groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"hosp_admits-{month_params[0]}"})
    )


def hosp_admissions(params):
    """
    Number of admissions to each hospital during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            f"""SELECT * FROM inpatient
                    WHERE admission_date BETWEEN ? AND ?;""",
            params,
        )
        .groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"admissions-{params[0][:7]}"})
    )


def resulting_30_day_hosp_count(params):
    """
    Number of admissions that result in a 30-day readmit by hospital during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.admissions_resulting_in_30day_df(params, "inpatient")
        .groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"results_in_30dr-{params[0][:7]}"})
    )


def readmit_30_day_hosp_count(params):
    """
    Number of admissions that are a 30-day readmit by hospital during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.admissions_30day_readmit_df(params, "inpatient")
        .groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"30dr-{params[0][:7]}"})
    )


def infections_by_hosp(params):
    """
    Number of infections that are acquired at a hospital by hospital during the time period.
    
    Looks for all infections that are indicated as being acquired at a hospital from
    the infections table. Then looks for any admissions within 7 days of the infection date
    and counts them per hospital.

    Any infections that can not be found to have an admission are saved to a csv file
    for further inspection.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    infections = u.dataframe_query(
        """SELECT * FROM infections
                        WHERE where_infection_was_acquired = 'Hospital'
                        AND date_time_occurred BETWEEN ? AND ?;""",
        params,
    )
    infections_with_hosp = f"""
                    SELECT infections.member_id, admission_date, discharge_date, facility, los, date_time_occurred
                    FROM infections
                    LEFT JOIN inpatient ut on infections.member_id=ut.member_id
                    WHERE where_infection_was_acquired = 'Hospital'
                    AND date_time_occurred BETWEEN ? AND ?;"""

    df = u.dataframe_query(infections_with_hosp, params)

    df["discharge_date"] = pd.to_datetime(df["discharge_date"]).dt.date
    df["date_time_occurred"] = pd.to_datetime(df["date_time_occurred"]).dt.date

    df["days_between_hosp_and_inf"] = abs(
        df["date_time_occurred"] - df["discharge_date"]
    )

    try:
        hosp_within_one_weeks = df[
            (df["days_between_hosp_and_inf"].dt.days <= 7)
        ].copy()

    except AttributeError:
        return pd.DataFrame.from_dict({"facility": ["None"], "infections": ["None"]})

    hosp_within_one_weeks.sort_values("days_between_hosp_and_inf", inplace=True)
    hosp_within_one_weeks.drop_duplicates(
        ["member_id", "date_time_occurred"], keep="first", inplace=True
    )
    infections[
        -infections["member_id"].isin(hosp_within_one_weeks["member_id"])
    ].to_csv("output/hospital_inf_without_visit.csv", index=False)
    return (
        hosp_within_one_weeks.groupby("facility")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": "infections"})
    )


def adc_census_on_date(params):
    """
    Number of ppts at each adult day center during the time period.

    Args:
        params(tuple): start date and end date in format 'YYYY-MM-DD'

    Returns:
        DataFrame: dataframe contains the facilities and indicator value
            for time period.    
    """
    return (
        u.dataframe_query(
            """SELECT * FROM authorizations
                WHERE service_type='Adult Day Center Attendance'
                AND (approval_expiration_date >= ?
                OR approval_expiration_date IS NULL)
                AND approval_effective_date <= ?""",
            params,
        )
        .groupby("vendor")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"adc_census-{params[0][:7]}"})
    )


def pressure_ulcers_at_facility(facility_type, params):
    params = [facility_type] + list(params)
    df = (
        u.dataframe_query(
            """SELECT * FROM wounds
                        WHERE living_situation IS ?
                        AND date_time_occurred BETWEEN ? AND ?""",
            params,
        )
        .groupby("living_detail")
        .count()["member_id"]
        .reset_index()
        .rename(columns={"member_id": f"{facility_type}_pulcers-{params[0][:7]}"})
    )
    df["living_detail"] = df["living_detail"].fillna("Unknown")
    return df


def archive_files():
    """
    Copies output files into an archive folder.
    Create a zip of the folder and then deleted the non-zipped folder.
    """
    try:
        shutil.rmtree(f"output/{pd.datetime.today().date()}_update", ignore_errors=True)
    except shutil.Error:
        pass

    shutil.copytree("output", f"output/{pd.datetime.today().date()}_update")

    shutil.make_archive(
        f"output/archive/{pd.datetime.today().date()}_update",
        "zip",
        f"output/{pd.datetime.today().date()}_update",
    )

    shutil.rmtree(f"output/{pd.datetime.today().date()}_update", ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start_date",
        default=u.last_quarter()[0],
        help="Start Date of period to run script for, formatted as YYYY-MM-DD.",
    )

    parser.add_argument(
        "--end_date",
        default=u.last_quarter()[1],
        help="End Date of period to run script for, formatted as YYYY-MM-DD.",
    )

    arguments = parser.parse_args()

    build_csv_date_spread(
        alf_census_on_date, "alfs", "facility_name", **vars(arguments)
    ).to_csv("output/alf_census.csv", index=False)
    build_csv_date_spread(
        alf_to_hosp, "alfs", "facility_name", **vars(arguments)
    ).to_csv("output/hosp_from_alf.csv", index=False)

    build_csv_date_spread(
        nf_census_on_date, "nursing_home", "facility", **vars(arguments)
    ).to_csv("output/nf_census.csv", index=False)
    build_csv_date_spread(
        nf_to_hosp, "nursing_home", "facility", **vars(arguments)
    ).to_csv("output/hosp_from_nf.csv", index=False)

    build_csv_date_spread(
        hosp_admissions, "inpatient", "facility", **vars(arguments)
    ).to_csv("output/hosp_admissions.csv", index=False)
    build_csv_date_spread(
        resulting_30_day_hosp_count, "inpatient", "facility", **vars(arguments)
    ).to_csv("output/hosp_admit_results_in_30day.csv", index=False)
    build_csv_date_spread(
        readmit_30_day_hosp_count, "inpatient", "facility", **vars(arguments)
    ).to_csv("output/hosp_30_day_readmits.csv", index=False)
    build_csv_date_spread(
        infections_by_hosp, "inpatient", "facility", **vars(arguments)
    ).to_csv("output/hosp_infections.csv", index=False)

    build_csv_date_spread(
        adc_census_on_date, "authorizations", "vendor", **vars(arguments)
    ).to_csv("output/adc_census.csv", index=False)

    build_pressure_wound_csv("SNF", **vars(arguments)).to_csv(
        "output/snf_pulcers.csv", index=False
    )
    build_pressure_wound_csv("ALF", **vars(arguments)).to_csv(
        "output/alf_pulcers.csv", index=False
    )

    archive_files()
