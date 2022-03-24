import pandas as pd
from datetime import datetime
import numpy as np 
import re
import os
import timeit

from sqlalchemy import false

start = timeit.default_timer()
fecha = datetime.today().strftime('%Y%m%d')
num_format = re.compile("^[\-]?[0-9]*\.?[0-9]+$")



# #--------------------------farm plots---------------------------------------------------------------------------------------------------------------
path = r'D:\AEPS 2.0 Boyaca\datos\BD_SISTEMAS_AGRÍCOLAS-SOCIOECONÓMICOS-CLIMÁTICO\BD CLIMÁTICAS'
files = os.listdir(path)
files_xlsm = [f for f in files if f[-4:] == 'xlsm']

#Data frame
df = pd.DataFrame()

for f in files_xlsm:
	path_ = os.path.join(path, f)
	data = pd.read_excel(path_, 'PRINCIPAL', skiprows=2)
	#data["ext_id_fram"] = "ESTACION-CLIMATICA-FINCA-" + data.iloc[:, 0].astype(str)
	data["ext_id_fram"] = 'FINCA-ESTACION-' + data.iloc[:, 0].astype(str)
	data["ext_id_plot"] = 'LOTE-ESTACION-' + data.iloc[:, 0].astype(str)
	data["name"] = 'Estacion Climatica # '  + data.iloc[:, 0].astype(str)
	data["farmer"]	= "ESTA-" + f.split("-")[2][:2]
	df = df.append(data)

f = {'name': df["name"], 'latitude': df["Latitud"], 'location_comments': df["Vereda"] , 'ext_id' : df['ext_id_fram'], 'farmer': df['farmer'],'longitude': df["Longitud"]}
far_farms = pd.DataFrame(f)
far_farms.to_csv('far_farms.csv', index=False)


p = {'farm': df['ext_id_fram'], 'name': df["name"], 'latitude': df["Latitud"], 'altitude': df['Altura'], 'ext_id': df ['ext_id_plot'], 'longitude': df["Longitud"]}
far_plots = pd.DataFrame(p)

far_plots.to_csv('far_plots.csv', index=False)

print(far_plots)

# #--------------------------farm plots---------------------------------------------------------------------------------------------------------------
print('iniciando con los datos') 
start = timeit.default_timer()

path = r'D:\AEPS 2.0 Boyaca\datos\BD_SISTEMAS_AGRÍCOLAS-SOCIOECONÓMICOS-CLIMÁTICO\BD CLIMÁTICAS'
files = os.listdir(path)
files_xlsm = [f for f in files if f[-4:] == 'xlsm']

#Data frame
df = pd.DataFrame()

for f in files_xlsm:
	path_ = os.path.join(path, f)
	if f == '1. BD-CLIMÁTICOS-MOTAVITA.xlsm':	
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 3', 'ESTACIÓN 4'])
		data['ESTACIÓN 3']['estacion'] = 3
		data['ESTACIÓN 4']['estacion'] = 4
		df = df.append(data['ESTACIÓN 3'])
		df = df.append(data['ESTACIÓN 4'])
		

	if f == '2. BD-CLIMÁTICOS-SAMACÁ.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 9', 'ESTACIÓN 7', 'ESTACIÓN 6'])
		data['ESTACIÓN 9']['estacion'] = 9
		data['ESTACIÓN 7']['estacion'] = 7
		data['ESTACIÓN 6']['estacion'] = 6
		df = df.append(data['ESTACIÓN 9'])
		df = df.append(data['ESTACIÓN 7'])
		df = df.append(data['ESTACIÓN 6'])

	if f == '3. BD-CLIMÁTICOS-SIACHOQUE.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 12', 'ESTACIÓN 16'])
		data['ESTACIÓN 12']['estacion'] = 12
		data['ESTACIÓN 16']['estacion'] = 16
		df = df.append(data['ESTACIÓN 12'])
		df = df.append(data['ESTACIÓN 16'])

	if f == '4. BD-CLIMÁTICOS-SORACÁ.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 1', 'ESTACIÓN 2'])
		data['ESTACIÓN 1']['estacion'] = 1
		data['ESTACIÓN 2']['estacion'] = 2
		df = df.append(data['ESTACIÓN 1'])
		df = df.append(data['ESTACIÓN 2'])

	if f == '5. BD-CLIMÁTICOS-TOCA.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 5', 'ESTACIÓN 15'])
		data['ESTACIÓN 5']['estacion'] = 5
		data['ESTACIÓN 15']['estacion'] = 15		
		df = df.append(data['ESTACIÓN 5'])
		df = df.append(data['ESTACIÓN 15'])

	if f == '6. BD-CLIMÁTICOS-TUNJA.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 13', 'ESTACIÓN 14'])
		data['ESTACIÓN 13']['estacion'] = 13
		data['ESTACIÓN 14']['estacion'] = 14
		df = df.append(data['ESTACIÓN 13'])
		df = df.append(data['ESTACIÓN 14'])

	if f == '7. BD-CLIMÁTICOS-VENTAQUEMADA.xlsm':
		data = pd.read_excel(path_, sheet_name = ['ESTACIÓN 11', 'ESTACIÓN 10', 'ESTACIÓN 8','ESTACIÓN 7'])
		data['ESTACIÓN 11']['estacion'] = 11
		data['ESTACIÓN 10']['estacion'] = 10
		data['ESTACIÓN 8']['estacion'] = 8
		data['ESTACIÓN 7']['estacion'] = 7
		df = df.append(data['ESTACIÓN 11'])
		df = df.append(data['ESTACIÓN 10'])
		df = df.append(data['ESTACIÓN 8'])
		df = df.append(data['ESTACIÓN 7'])



df.columns = ['date',
'time',
'temp_ext',
'temp_high',
'temp_low',
'ext_rela_hum',
'dew_point',
'wind_speed',
'wind_dire',
'wind_travel',
'high_speed',
'dir_trend',
'thermal_sen',
'heat_index',
'tem_hum_wind',
'tem_hum_wind_sun',
'atm_press',
'precipi',
'ppt_rate',
'solar_radi',
'solar_ener',
'high_solar_radi',
'degree_days_heat',
'degree_days_cold',
'console_temp',
'console_rela_hum',
'console_dew_point',
'console_heat_index',
'degrees_days_cooling', 
'air_density',
'evapo',
'sample_wind_speed',
'wind_channel',
'minutes',
'interval_file',
'estacion'
]

df = df.drop_duplicates() #Se eliminan los registros que puedan estar duplicados

df['event'] =  range(0, df.shape[0])
df['event'] = 'EVENTO-CLI-' + df['estacion'].astype(str) + '-' + fecha + '-' + df['event'].astype(str)



df['ext_id_plot'] = 'LOTE-ESTACION-' + df['estacion'].astype(str)


# far_production_events
# far_production_events = pd.DataFrame({'technical' : 1, 'ext_id' : df['ext_id_event'], 'plot': df['ext_id_plot'], 'form' : 2})
far_production_events = pd.DataFrame({'technical' : 1, 'ext_id' : df['event'], 'plot': df['ext_id_plot'], 'form' : 2, 'enable':1})
# far_production_events = pd.DataFrame({'technical' : 1, 'ext_id' : df['ext_id_event'], 'plot': df['estacion'], 'form' : 2, 'farm':df['estacion']})



far_production_events.to_csv('far_production_events.csv', index=False)


df = df.replace("---", "")
df = df.replace("------", "")

df = df.drop(['estacion', 'ext_id_plot'], axis=1)

df.to_csv("far_weather.csv", index=False)

stop = timeit.default_timer()

print('Time: ', stop - start) 

