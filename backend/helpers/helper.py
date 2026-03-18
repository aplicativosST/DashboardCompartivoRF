'''
****************************************** Helper functions for the backend ****************************************
Description:
    File containing helper functions for the backend.

List of functions:
    extract_sheet_names(rf_scenarios:Dict[str,str])->List[str]: Extracts the common sheet names of the comparative scenarios. If there are no common scenarios it raises error.

    extract_data(rf_scenarios:Dict[str,str]) ->Dict: Extracts the information from given consolidados files, and parse it to json format.
        monthly_dict_data_extraction(monthly_dict_data_extraction(raw_data:pd.DataFrame) -> Dict: Used within extract_data to extract monthly information.
        yearly_dict_data_extraction(monthly_dict_data_extraction(raw_data:pd.DataFrame) -> Dict: Used within extract_data to extract yearly information.
        
    spot_monthly_data_extraction(monthly_results,scenario:str)->Dict: Extracts the spot information for the case comparison dashboard.

    variable_monthly_data_extraction(monthly_results,scenario:str,var_name:str,plant:str)->Dict: Extracts a given variable information for a given plant and scenario. Note that plant can take the value of "SIN" for certain variables.

    delta_gx_table(monthly_results,scenario:str,base_case:str,compare_case:str)->Dict: Returns the comparative delta table of Gx between a compare case and a base case for a given scenario.
    
*******************************************************************************************************************
'''

#%% Libraries
import pandas as pd
import json
import numpy as np
import typing
from typing import List,Dict
import os
import datetime
import openpyxl
from openpyxl import load_workbook

np.set_printoptions(suppress=True, precision=2)

#%% Helper functions for data extraction and parsing

def tolist(df_slice):
    '''
    Description
        Converts a given slice of a dataframe to a list. This is used to pass data in json format to FastApi
    '''
    return df_slice.to_numpy().tolist()


def extract_sheet_names(rf_scenarios:Dict[str,str])->List[str]:
    '''
    Description:
        Extracts the sheet names from the scenarios to compare. If the scenarios do not have any sheet in common raises RunTimeError.
    
    Args:
        rf_scenarios (dict): Dictionary containing the alias and routes of RF scenarios to compare.
    
    Returns:
        sheet_names (list): List of sheet names that are common to all scenarios to compare.
    '''

    if rf_scenarios is None or len(rf_scenarios)==0:
        raise RuntimeError("No se han proporcionado escenarios para comparar. Por favor, revise las entradas")
    sheet_names_dict={}    # Setting dictionary to store the sheet names for each scenario for final comparison
    all_common_sheets=[]      # List of common sheets between scenarios relative to compare scenario

    # Get the sheet names for compare case 
    for file_alias,items in rf_scenarios.items():
        try:
            pd_excel=pd.ExcelFile(items["route"])
        except Exception as e:
             raise RuntimeError(f"Error abriendo el archivo {items["route"]} :{e}")
        if items["compare_case"]:            
            sheet_names_dict["compare_case"]=[sheet if "escenario_" in sheet.strip().lower() else None for sheet in pd_excel.sheet_names ]
        else:
            sheet_names_dict[f"{file_alias}"]=[sheet if "escenario_" in sheet.strip().lower() else None for sheet in pd_excel.sheet_names ]

    # Validate the existance of common sheets between scenarios
    for alias,sheets in sheet_names_dict.items():
        if alias !="compare_case":
            common_sheets=[sheet if sheet in sheet_names_dict["compare_case"] else None for sheet in sheets]
            all_common_sheets=all_common_sheets+common_sheets

    # Removing None values from the list of common sheets
    all_common_sheets=[sheet for sheet in all_common_sheets if sheet is not None]

    #If there are no common sheets raise error, else return common sheets
    if len(all_common_sheets)==0:
        raise RuntimeError("No hay escenarios en común entre los archivos rf a comparar. Por favor, revise las fuentes de información")
    else:
        return sorted(list(set(all_common_sheets)))


def extract_data(rf_scenarios:Dict[str,str],sheet_names:List[str]) ->Dict:
    '''
    Description:
        Extracts the data from a given consolidado file to form the required variable information
        
    Args:
        rf_scenarios (dict): Dictionary containing the alias and routes of RF scenarios to compare.
        sheet_names (list): List of all sheet names in common to compare between the RF scenarios.
    
    Returns:
        compiled_monthly (dict): Dictionary containing all Scenario information for the consolidado scenarios to compare

    '''

    # Create the empty dictionary to concatenate all scenarios
    compiled_monthly={} 
    compiled_yearly={}
    
    # Raw Data extraction
    for alias,item in rf_scenarios.items():
        alias_montly_data,alias_yearly_data={},{}# Creating an initial clean dictionary to store data
        route=item["route"] #Extracting the route information for the scenario
        for sheet in sheet_names:
            # Reading the data
            raw_data=pd.read_excel(route,sheet_name=sheet)
            raw_data=raw_data.T #Transpose for filtering purposes
            raw_data.rename(columns={0:"date"},inplace=True)
            raw_data.drop("Unnamed: 0",axis="index",inplace=True)

            alias_montly_data[sheet]=monthly_dict_data_extraction(raw_data)
            alias_yearly_data[sheet]=yearly_dict_data_extraction(raw_data)
        
        compiled_monthly[alias]=alias_montly_data
        compiled_yearly[alias]=alias_yearly_data
    return compiled_monthly,compiled_yearly

def monthly_dict_data_extraction(raw_data:pd.DataFrame) -> Dict:
    ''' 
    Description:
        Forms the monthly dictionary data by extracting rows from dataframe "raw_data"

    Args:
        raw_data (df): Dataframe containing the information from a given scenario

    Returns
        monthly_dict (dict): Dictionary containing the monthly data from a given scenario 
    '''

    monthly_dict = {
        "spot": tolist(raw_data.iloc[0:24,2]),
        "date": tolist(raw_data.iloc[0:24,0]),

        "gx": {
            "guavio": tolist(raw_data.iloc[0:24,13]),
            "quimbo": tolist(raw_data.iloc[0:24,21]),
            "betania": tolist(raw_data.iloc[0:24,29]),
            "pagua": tolist(raw_data.iloc[0:24,39]),
            "menores": tolist(raw_data.iloc[0:24,41]),
            "filo": tolist(raw_data.iloc[0:24,40]),
            "total_rb": tolist(raw_data.iloc[0:24,40]),
            "guavio_menor": tolist(raw_data.iloc[0:24,42]),

            "total_hidro": (
                raw_data.iloc[0:24,13]
                + raw_data.iloc[0:24,21]
                + raw_data.iloc[0:24,29]
                + raw_data.iloc[0:24,40]
                + raw_data.iloc[0:24,42]
            ).to_numpy().tolist(),

            "zipa_2": tolist(raw_data.iloc[0:24,44]),
            "zipa_3": tolist(raw_data.iloc[0:24,45]),
            "zipa_4": tolist(raw_data.iloc[0:24,46]),
            "zipa_5": tolist(raw_data.iloc[0:24,47]),
            "zipa_total": tolist(raw_data.iloc[0:24,43]),

            "atlantico": tolist(raw_data.iloc[0:24,49]),
            "guayepo_12": tolist(raw_data.iloc[0:24,50]),
            "guayepo_3": tolist(raw_data.iloc[0:24,51]),
            "fundacion": tolist(raw_data.iloc[0:24,54]),
            "la_loma": tolist(raw_data.iloc[0:24,52]),
            "el_paso": tolist(raw_data.iloc[0:24,53]),
            "solar_total": tolist(raw_data.iloc[0:24,55]),
        },

        "embalses": {
            "guavio": tolist(raw_data.iloc[0:24,15]),
            "quimbo": tolist(raw_data.iloc[0:24,23]),
            "betania": tolist(raw_data.iloc[0:24,31]),
            "sin": tolist(raw_data.iloc[0:24,84]),
        },

        "aportes": {
            "sin": tolist(raw_data.iloc[0:24,83]),
            "guavio": tolist(raw_data.iloc[0:24,12]),
            "quimbo": tolist(raw_data.iloc[0:24,20]),
            "betania": tolist(raw_data.iloc[0:24,30]),
            "pagua": tolist(raw_data.iloc[0:24,37]),
        }
    }

    return monthly_dict

def yearly_dict_data_extraction(raw_data:pd.DataFrame) -> Dict:
    ''' 
    Description:
        Forms the monthly dictionary data by extracting rows from dataframe "raw_data"

    Args:
        raw_data (df): Dataframe containing the information from a given scenario

    Returns
        yearly_dict (dict): Dictionary containing the yearly data from a given scenario 
    '''

    yearly_dict = {
        "spot": tolist(raw_data.iloc[25:27,2]),
        "date": tolist(raw_data.iloc[25:27,0]),

        "gx": {
            "guavio": tolist(raw_data.iloc[25:27,13]),
            "quimbo": tolist(raw_data.iloc[25:27,21]),
            "betania": tolist(raw_data.iloc[25:27,29]),
            "pagua": tolist(raw_data.iloc[25:27,39]),
            "menores": tolist(raw_data.iloc[25:27,41]),
            "filo": tolist(raw_data.iloc[25:27,40]),
            "total_rb": tolist(raw_data.iloc[25:27,40]),
            "guavio_menor": tolist(raw_data.iloc[25:27,42]),

            "total_hidro": (
                raw_data.iloc[25:27,13]
                + raw_data.iloc[25:27,21]
                + raw_data.iloc[25:27,29]
                + raw_data.iloc[25:27,40]
                + raw_data.iloc[25:27,42]
            ).to_numpy().tolist(),

            "zipa_2": tolist(raw_data.iloc[25:27,44]),
            "zipa_3": tolist(raw_data.iloc[25:27,45]),
            "zipa_4": tolist(raw_data.iloc[25:27,46]),
            "zipa_5": tolist(raw_data.iloc[25:27,47]),
            "zipa_total": tolist(raw_data.iloc[25:27,43]),

            "atlantico": tolist(raw_data.iloc[25:27,49]),
            "guayepo_12": tolist(raw_data.iloc[25:27,50]),
            "guayepo_3": tolist(raw_data.iloc[25:27,51]),
            "fundacion": tolist(raw_data.iloc[25:27,54]),
            "la_loma": tolist(raw_data.iloc[25:27,52]),
            "el_paso": tolist(raw_data.iloc[25:27,53]),
            "solar_total": tolist(raw_data.iloc[25:27,55]),
        },

        "embalses": {
            "guavio": tolist(raw_data.iloc[25:27,15]),
            "quimbo": tolist(raw_data.iloc[25:27,23]),
            "betania": tolist(raw_data.iloc[25:27,31]),
            "sin": tolist(raw_data.iloc[25:27,84]),
        },

        "aportes": {
            "sin": tolist(raw_data.iloc[25:27,83]),
            "guavio": tolist(raw_data.iloc[25:27,12]),
            "quimbo": tolist(raw_data.iloc[25:27,20]),
            "betania": tolist(raw_data.iloc[25:27,30]),
            "pagua": tolist(raw_data.iloc[25:27,37]),
        }
    }
    return yearly_dict


def spot_monthly_data_extraction(monthly_results,scenario:str)->Dict:
    '''
    Description:
        Creates a bokeh plot for each scenario in the monthly results dictionary
    
    Args:
        monthly_results (dict): Dictionary containing all Scenario information for the consolidado scenarios to compare
        scenario (str): The name of the scenario to extract data for (e.g., "Escenario_1")
    
    Returns:
        all_plots (list): List of bokeh figures, one for each spot scenario
    '''
    # Set up the empty dictionary to store the extracted data
    spot_data={}

    # Insert the spot data for each case and scenario into the output dictionary
    for case in monthly_results.keys():
        spot_data[case]={
            "date":monthly_results[case][scenario]["date"],
            "spot":monthly_results[case][scenario]["spot"]
        }

    return spot_data

def variable_monthly_data_extraction(monthly_results,scenario:str,var_name:str,plant:str)->Dict:
    '''
    Description:
        Extracts the variable data from the monthly results dictionary for a given scenario, variable name, and plant.
    
    Args:
        monthly_results (dict): Dictionary containing all Scenario information for the consolidado scenarios to compare
        scenario (str): The name of the scenario to extract data for (e.g., "Escenario_1")
        var_name (str): The name of the variable to extract (e.g., "gx", "embalses", "aportes")
        plant (str): The name of the plant to extract data for (e.g., "guavio")
    
    Returns:
        variable_data (dict): Dictionary containing the extracted variable data for the specified scenario, variable name, and plant
    '''

    #Set up the empty dictionary to store the extracted data
    variable_data={}

    for case in monthly_results.keys():
        variable_data[case]={
            "date":monthly_results[case][scenario]["date"],
            "variable":monthly_results[case][scenario][var_name][plant]
        }

    return variable_data




def spot_yearly_data_extraction(yearly_results,scenario:str)->Dict:
    '''
    Description:
        Creates a bokeh plot for each scenario in the yearly results dictionary
    
    Args:
        yearly_results (dict): Dictionary containing all Scenario information for the consolidado scenarios to compare
        scenario (str): The name of the scenario to extract data for (e.g., "Escenario_1")
    
    Returns:
        all_plots (list): List of bokeh figures, one for each spot scenario
    '''
    # Set up the empty dictionary to store the extracted data
    spot_data={}

    # Insert the spot data for each case and scenario into the output dictionary
    for case in yearly_results.keys():
        spot_data[case]={
            "date":yearly_results[case][scenario]["date"],
            "spot":yearly_results[case][scenario]["spot"]
        }
    return spot_data

def variable_yearly_data_extraction(yearly_results,scenario:str,var_name:str,plant:str)->Dict:
    '''
    Description:
        Extracts the variable data from the yearly results dictionary for a given scenario, variable name, and plant.
    
    Args:
        yearly_results (dict): Dictionary containing all Scenario information for the consolidado scenarios to compare
        scenario (str): The name of the scenario to extract data for (e.g., "Escenario_1")
        var_name (str): The name of the variable to extract (e.g., "gx", "embalses", "aportes")
        plant (str): The name of the plant to extract data for (e.g., "guavio")
    
    Returns:
        variable_data (dict): Dictionary containing the extracted variable data for the specified scenario, variable name, and plant
    '''

    #Set up the empty dictionary to store the extracted data
    variable_data={}

    for case in yearly_results.keys():
        variable_data[case]={
            "date":yearly_results[case][scenario]["date"],
            "variable":yearly_results[case][scenario][var_name][plant]
        }

    return variable_data






