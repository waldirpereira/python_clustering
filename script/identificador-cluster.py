import os
import sys, warnings
import pandas as pd
import numpy as np
import json

# Suppress all kinds of warnings (this would lead to an exception on the client side).
warnings.simplefilter("ignore")

from transformacaoDadosService import TransformacaoDadosService
transformacaoDadosService = TransformacaoDadosService()

jsonData = sys.argv[1]
path = sys.argv[2]

data = json.loads(jsonData)
df = pd.json_normalize(data)

df_ajustado = transformacaoDadosService.geraDataFrameAgrupadoPorPoco(df)
df_categorico = transformacaoDadosService.transformaDadosCategoricos(df_ajustado)
df_numerico = transformacaoDadosService.transformaDadosCategoricosEmDadosNumericos(df_categorico)

import pickle

#filename = '{}/finalized_model.sav'.format(path)
filename = 'D:\\projects\\cronoweb\\src\\source\\CronogramaDeSondas.WebMvc\\bin\\Python\\Scripts\\finalized_model.sav'
filehandler = open(filename, 'rb')
loaded_model = pickle.load(filehandler)
filehandler.close()

valores = df_numerico.values

tamanho_inicial = valores.shape[1]
tamanho_objetivo = len(loaded_model.labels_)
i = 0
for i in range(tamanho_objetivo - tamanho_inicial + 1):
    valores = np.append(valores, 0)

df_to_fit = [valores]

result_saved_model_cluster = loaded_model.predict(df_to_fit)

print(result_saved_model_cluster[0])