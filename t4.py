import requests
import xml.etree.ElementTree as et
import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

indicadores = ["Number of deaths", "Number of infant deaths", "Number of under-five deaths", \
    "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)", \
    "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)", \
    "Estimates of number of homicides",  "Crude suicide rates (per 100 000 population)", \
    "Mortality rate attributed to unintentional poisoning (per 100 000 population)", \
    "Number of deaths attributed to non-communicable diseases, by type of disease and sex", \
    "Estimated road traffic death rate (per 100 000 population)", "Estimated number of road traffic deaths", \
    "Mean BMI (crude estimate)", "Mean BMI (age-standardized estimate)", \
    "Prevalence of obesity among adults, BMI > 30 (age-standardized estimate) (%)", \
    "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)", \
    "Prevalence of overweight among adults, BMI > 25 (age-standardized estimate) (%)", \
    "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)", \
    "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",          
    "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)", \
    "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)", \
    "Estimate of daily cigarette smoking prevalence (%)", "Estimate of daily tobacco smoking prevalence (%)",
    "Estimate of current cigarette smoking prevalence (%)", "Estimate of current tobacco smoking prevalence (%)", \
    "Mean systolic blood pressure (crude estimate)", "Mean fasting blood glucose (mmol/l) (crude estimate)", \
    "Mean Total Cholesterol (crude estimate)"]


no_exactos = ["Mean BMI", "Prevalence of obesity among adults", "Prevalence of overweight among adults"]


def ver_parecido(text):
    for frase in no_exactos:
        if frase in text:
            return True

    return False


# sacado de https://medium.com/@robertopreste/from-xml-to-pandas-dataframes-9292980b1c1c
def parse_XML(xml_file, df_cols, df): 
    xroot = et.fromstring(xml_file)
    rows = []
    
    for node in xroot: 
        res = []
        if node.find("GHO").text in indicadores or ver_parecido(node.find("GHO").text):
            for el in df_cols: 
                if node is not None and node.find(el) is not None:
                    if el == 'Numeric' or el == 'Low' or el == 'High':
                        res.append(float(node.find(el).text))
                    else:     
                        res.append(node.find(el).text)
                else: 
                    res.append(None)
            rows.append({df_cols[i]: res[i] 
                     for i, _ in enumerate(df_cols)})
    
    out_df = df.append(rows)
        
    return out_df

gc = gspread.service_account(filename='credenciales.json')
sh = gc.open_by_key('1cIlygNc6Ke_w0gvyDl8JU8-J3LvGgm5FF7JeQ0Hfhp4')

df = pd.DataFrame(columns= ["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "Display", "Numeric", "Low", "High"])
codigos = ['CHL', 'ARG', 'COL', 'BRA', 'PER', 'PRY']
hoja = 0
for codigo in codigos:
    r = requests.get(f'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{codigo}.xml')
    df = parse_XML(r.content, ["GHO", "COUNTRY", "SEX", "YEAR", "GHECAUSES", "AGEGROUP", "Display", "Numeric", "Low", "High"], df)    
    worksheet = sh.get_worksheet(0)
    set_with_dataframe(worksheet, df)    
    hoja += 1






