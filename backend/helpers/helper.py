#%% Libraries
import pandas as pd
import numpy as np
from typing import List, Dict, Any

np.set_printoptions(suppress=True, precision=2)


#%% Helper functions for data extraction and parsing

def tolist(df_slice):
    """
    Converts a dataframe slice or series to a Python list.
    """
    return df_slice.to_numpy().tolist()


def extract_sheet_names(rf_scenarios: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Extracts the common sheet names between the compare case and the other uploaded scenarios.
    Only sheets containing 'escenario_' are considered valid.
    """

    if not rf_scenarios:
        raise RuntimeError(
            "No se han proporcionado escenarios para comparar. Por favor, revise las entradas"
        )

    sheet_names_dict = {}
    all_common_sheets = []

    for file_alias, items in rf_scenarios.items():
        try:
            pd_excel = pd.ExcelFile(items["route"])
        except Exception as e:
            raise RuntimeError(f'Error abriendo el archivo {items["route"]}: {e}')

        valid_sheets = [
            sheet for sheet in pd_excel.sheet_names
            if "escenario_" in sheet.strip().lower()
        ]

        if items["compare_case"]:
            sheet_names_dict["compare_case"] = valid_sheets
        else:
            sheet_names_dict[file_alias] = valid_sheets

    if "compare_case" not in sheet_names_dict:
        raise RuntimeError(
            "No se encontró un caso base para comparar. Por favor, revise las entradas"
        )

    for alias, sheets in sheet_names_dict.items():
        if alias != "compare_case":
            common_sheets = [sheet for sheet in sheets if sheet in sheet_names_dict["compare_case"]]
            all_common_sheets.extend(common_sheets)

    all_common_sheets = [sheet for sheet in all_common_sheets if sheet is not None]

    if len(all_common_sheets) == 0:
        raise RuntimeError(
            "No hay escenarios en común entre los archivos RF a comparar. Por favor, revise las fuentes de información"
        )

    return sorted(list(set(all_common_sheets)))


def extract_data(
    rf_scenarios: Dict[str, Dict[str, Any]],
    sheet_names: List[str]
) -> Dict:
    """
    Extracts monthly and yearly data from the selected sheet names for each scenario.
    """

    compiled_monthly = {}
    compiled_yearly = {}

    for alias, item in rf_scenarios.items():
        alias_monthly_data = {}
        alias_yearly_data = {}
        route = item["route"]

        for sheet in sheet_names:
            raw_data = pd.read_excel(route, sheet_name=sheet)
            raw_data = raw_data.T
            raw_data.rename(columns={0: "date"}, inplace=True)
            raw_data.drop("Unnamed: 0", axis="index", inplace=True)

            alias_monthly_data[sheet] = monthly_dict_data_extraction(raw_data)
            alias_yearly_data[sheet] = yearly_dict_data_extraction(raw_data)

        compiled_monthly[alias] = alias_monthly_data
        compiled_yearly[alias] = alias_yearly_data

    return compiled_monthly, compiled_yearly


def monthly_dict_data_extraction(raw_data: pd.DataFrame) -> Dict:
    monthly_dict = {
        "spot": tolist(raw_data.iloc[0:24, 2]),
        "date": tolist(raw_data.iloc[0:24, 0]),

        "gx": {
            "guavio": tolist(raw_data.iloc[0:24, 13]),
            "quimbo": tolist(raw_data.iloc[0:24, 21]),
            "betania": tolist(raw_data.iloc[0:24, 29]),
            "pagua": tolist(raw_data.iloc[0:24, 39]),
            "menores": tolist(raw_data.iloc[0:24, 41]),
            "filo": tolist(raw_data.iloc[0:24, 40]),
            "total_rb": tolist(raw_data.iloc[0:24, 40]),
            "guavio_menor": tolist(raw_data.iloc[0:24, 42]),

            "total_hidro": (
                raw_data.iloc[0:24, 13]
                + raw_data.iloc[0:24, 21]
                + raw_data.iloc[0:24, 29]
                + raw_data.iloc[0:24, 40]
                + raw_data.iloc[0:24, 42]
            ).to_numpy().tolist(),

            "zipa_2": tolist(raw_data.iloc[0:24, 44]),
            "zipa_3": tolist(raw_data.iloc[0:24, 45]),
            "zipa_4": tolist(raw_data.iloc[0:24, 46]),
            "zipa_5": tolist(raw_data.iloc[0:24, 47]),
            "zipa_total": tolist(raw_data.iloc[0:24, 43]),

            "atlantico": tolist(raw_data.iloc[0:24, 49]),
            "guayepo_12": tolist(raw_data.iloc[0:24, 50]),
            "guayepo_3": tolist(raw_data.iloc[0:24, 51]),
            "fundacion": tolist(raw_data.iloc[0:24, 54]),
            "la_loma": tolist(raw_data.iloc[0:24, 52]),
            "el_paso": tolist(raw_data.iloc[0:24, 53]),
            "solar_total": tolist(raw_data.iloc[0:24, 55]),
        },

        "embalses": {
            "guavio": tolist(raw_data.iloc[0:24, 15]),
            "quimbo": tolist(raw_data.iloc[0:24, 23]),
            "betania": tolist(raw_data.iloc[0:24, 31]),
            "sin": tolist(raw_data.iloc[0:24, 84]),
        },

        "aportes": {
            "sin": tolist(raw_data.iloc[0:24, 83]),
            "guavio": tolist(raw_data.iloc[0:24, 12]),
            "quimbo": tolist(raw_data.iloc[0:24, 20]),
            "betania": tolist(raw_data.iloc[0:24, 30]),
            "pagua": tolist(raw_data.iloc[0:24, 37]),
        }
    }

    return monthly_dict


def yearly_dict_data_extraction(raw_data: pd.DataFrame) -> Dict:
    yearly_dict = {
        "spot": tolist(raw_data.iloc[25:27, 2]),
        "date": tolist(raw_data.iloc[25:27, 0]),

        "gx": {
            "guavio": tolist(raw_data.iloc[25:27, 13]),
            "quimbo": tolist(raw_data.iloc[25:27, 21]),
            "betania": tolist(raw_data.iloc[25:27, 29]),
            "pagua": tolist(raw_data.iloc[25:27, 39]),
            "menores": tolist(raw_data.iloc[25:27, 41]),
            "filo": tolist(raw_data.iloc[25:27, 40]),
            "total_rb": tolist(raw_data.iloc[25:27, 40]),
            "guavio_menor": tolist(raw_data.iloc[25:27, 42]),

            "total_hidro": (
                raw_data.iloc[25:27, 13]
                + raw_data.iloc[25:27, 21]
                + raw_data.iloc[25:27, 29]
                + raw_data.iloc[25:27, 40]
                + raw_data.iloc[25:27, 42]
            ).to_numpy().tolist(),

            "zipa_2": tolist(raw_data.iloc[25:27, 44]),
            "zipa_3": tolist(raw_data.iloc[25:27, 45]),
            "zipa_4": tolist(raw_data.iloc[25:27, 46]),
            "zipa_5": tolist(raw_data.iloc[25:27, 47]),
            "zipa_total": tolist(raw_data.iloc[25:27, 43]),

            "atlantico": tolist(raw_data.iloc[25:27, 49]),
            "guayepo_12": tolist(raw_data.iloc[25:27, 50]),
            "guayepo_3": tolist(raw_data.iloc[25:27, 51]),
            "fundacion": tolist(raw_data.iloc[25:27, 54]),
            "la_loma": tolist(raw_data.iloc[25:27, 52]),
            "el_paso": tolist(raw_data.iloc[25:27, 53]),
            "solar_total": tolist(raw_data.iloc[25:27, 55]),
        },

        "embalses": {
            "guavio": tolist(raw_data.iloc[25:27, 15]),
            "quimbo": tolist(raw_data.iloc[25:27, 23]),
            "betania": tolist(raw_data.iloc[25:27, 31]),
            "sin": tolist(raw_data.iloc[25:27, 84]),
        },

        "aportes": {
            "sin": tolist(raw_data.iloc[25:27, 83]),
            "guavio": tolist(raw_data.iloc[25:27, 12]),
            "quimbo": tolist(raw_data.iloc[25:27, 20]),
            "betania": tolist(raw_data.iloc[25:27, 30]),
            "pagua": tolist(raw_data.iloc[25:27, 37]),
        }
    }

    return yearly_dict


def spot_monthly_data_extraction(monthly_results, scenario: str) -> Dict:
    spot_data = {}

    for case in monthly_results.keys():
        spot_data[case] = {
            "date": monthly_results[case][scenario]["date"],
            "spot": monthly_results[case][scenario]["spot"]
        }

    return spot_data


def variable_monthly_data_extraction(monthly_results, scenario: str, var_name: str, plant: str) -> Dict:
    variable_data = {}

    for case in monthly_results.keys():
        variable_data[case] = {
            "date": monthly_results[case][scenario]["date"],
            "value": monthly_results[case][scenario][var_name][plant]
        }

    return variable_data


def spot_yearly_data_extraction(yearly_results, scenario: str) -> Dict:
    spot_data = {}

    for case in yearly_results.keys():
        spot_data[case] = {
            "date": yearly_results[case][scenario]["date"],
            "spot": yearly_results[case][scenario]["spot"]
        }

    return spot_data


def variable_yearly_data_extraction(yearly_results, scenario: str, var_name: str, plant: str) -> Dict:
    variable_data = {}

    for case in yearly_results.keys():
        variable_data[case] = {
            "date": yearly_results[case][scenario]["date"],
            "value": yearly_results[case][scenario][var_name][plant]
        }

    return variable_data