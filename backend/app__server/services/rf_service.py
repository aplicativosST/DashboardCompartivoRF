from fastapi import HTTPException

from helpers.helper import (
    extract_sheet_names,
    extract_data,
    spot_monthly_data_extraction,
    variable_monthly_data_extraction,
    spot_yearly_data_extraction,
    variable_yearly_data_extraction,
)


def get_sheet_names_service(session: dict):
    return extract_sheet_names(session["scenarios"])


def extract_excel_data_service(session: dict, sheet_names: list[str]):
    monthly_results, yearly_results = extract_data(session["scenarios"], sheet_names)
    session["monthly"] = monthly_results
    session["yearly"] = yearly_results


def extract_monthly_spot_service(session: dict, scenario: str):
    if session["monthly"] is None:
        raise HTTPException(status_code=404, detail="Monthly results not found")

    validate_scenario(session, scenario)
    return spot_monthly_data_extraction(session["monthly"], scenario)


def extract_monthly_var_service(session: dict, scenario: str, var_name: str, plant: str):
    if session["monthly"] is None:
        raise HTTPException(status_code=404, detail="Monthly results not found")

    validate_scenario(session, scenario)
    return variable_monthly_data_extraction(
        session["monthly"],
        scenario,
        var_name,
        plant,
    )


def extract_yearly_spot_service(session: dict, scenario: str):
    if session["yearly"] is None:
        raise HTTPException(status_code=404, detail="Yearly results not found")

    validate_scenario(session, scenario)
    return spot_yearly_data_extraction(session["yearly"], scenario)


def extract_yearly_var_service(session: dict, scenario: str, var_name: str, plant: str):
    if session["yearly"] is None:
        raise HTTPException(status_code=404, detail="Yearly results not found")

    validate_scenario(session, scenario)
    return variable_yearly_data_extraction(
        session["yearly"],
        scenario,
        var_name,
        plant,
    )


def validate_scenario(session: dict, scenario: str):
    if not scenario:
        raise HTTPException(status_code=400, detail="Scenario not found")

    if scenario not in session["scenarios"]:
        raise HTTPException(
            status_code=400,
            detail=f"Scenario '{scenario}' is not available in this session"
        )