import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class TransformacaoDadosService:
    
    def __init__(self):
        print("Inicializando o TransformacaoDadosService.")

    def importaPlanilhaEGeraDataFrameComDados(self, caminhoPlanilha, nomeAba):
        xls = pd.ExcelFile(caminhoPlanilha)
        df = xls.parse(nomeAba, skiprows=3, index_col=None)

        # Eliminação das duas últimas linhas do relatório (uma em branco e outra com informação de quem gerou o relatório)
        df = df[0:-2]
        return df

    def mantemApenasAtividadesPerfuracao(self, df_completo):
        df_somente_perfuracao = df_completo[df_completo['Tipo de serviço'] == 'Perfuração']
        return df_somente_perfuracao

    def possuiValor(self, valor):
        def search(list, value):
            for i in range(len(list)):
                if str(value).upper() in str(list[i]).upper():
                    return True
            return False
        def ipf(x):
            return (search(list(x), valor))
        ipf.__name__ = 'ipf {}'.format(str(valor))
        return ipf 

    def geraDataFrameAgrupadoPorPoco(self, df_a_ser_agrupado):
        # cria nova coluna copiada de Formacao
        df_a_ser_agrupado['Formação - cópia'] = df_a_ser_agrupado['Formação']

        group = df_a_ser_agrupado.groupby(by='Ponto geográfico').agg({
            #'Prefixo do recurso': ['count'], 
            #'Fase da atividade': ['nunique'],
            'Projeto': ['min'],
            'Atividade contingente': [self.possuiValor('Sim')],
            'Duração planejada': ['sum'],
            'Coordenador CIPP': ['min'],
            'Ativo': ['min'],
            'Bloco/Campo': ['min'],
            'Tipo de óleo': ['min'],
            'Região/Bacia': ['min'],
            'Natureza do projeto': ['min'],
            'Tipo de locação': ['min'],
            'Projeto pré-sal': [self.possuiValor('Sim')],
            'Cessão onerosa': [self.possuiValor('Sim')],
            'SICAR': ['min'],
            'Rodada ANP': ['min'],
            'Formação': [self.possuiValor('CARBONATO')],
            'Formação - cópia': [self.possuiValor('ARENITO')],
            'LDA': ['max'],
            'Demandante': ['min'],
            'Tipo de completação inferior': ['max'],
            'Demanda CATS': [self.possuiValor('Firme')],
            'Necessidade de SCC': [self.possuiValor('Sim')]
        })

        # renomeando colunas
        group.columns = [
            #'Qtd de atividades', 
            #'Qtd de fases',
            'Projeto',
            'Possui contingente',
            'Duração',
            'Coordenador CIPP',
            'Ativo',
            'Bloco/Campo',
            'Tipo de óleo',
            'Região/Bacia',
            'Natureza do projeto',
            'Tipo de locação',
            'Projeto pré-sal',
            'Cessão onerosa',
            'SICAR',
            'Rodada ANP',
            'Formação possui Carbonato',
            'Formação possui Arenito',
            'LDA Max',
            'Demandante',
            'Tipo de completação inferior',
            'Demanda CATS Firme',
            'Necessita SCC'
        ]

        # criação de um novo dataframe com os índices e colunas ajustados
        df_agrupado = group.reset_index()
        return df_agrupado

    def transformaDadosCategoricos(self, df_a_ser_transformado):
        #Ponto Geográfico
        df_a_ser_transformado['Poço Injetor'] = df_a_ser_transformado['Ponto geográfico'].apply(lambda s: '-I' in s[-3:] or '.I' in s[-3:] or '8-' in s[0:2])
        df_a_ser_transformado['Poço Produtor'] = df_a_ser_transformado['Ponto geográfico'].apply(lambda s: '-P' in s[-3:] or '.P' in s[-3:] or '7-' in s[0:2])
        df_a_ser_transformado['Poço Especial'] = df_a_ser_transformado['Ponto geográfico'].apply(lambda s: '9-' in s[0:2])

        #Dados categóricos
        df_a_ser_transformado['Projeto'] = df_a_ser_transformado['Projeto'].astype('category')
        df_a_ser_transformado['Coordenador CIPP'] = df_a_ser_transformado['Coordenador CIPP'].astype('category')
        df_a_ser_transformado['Ativo'] = df_a_ser_transformado['Ativo'].astype('category')
        df_a_ser_transformado['Bloco/Campo'] = df_a_ser_transformado['Bloco/Campo'].astype('category')
        df_a_ser_transformado['Tipo de óleo'] = df_a_ser_transformado['Tipo de óleo'].astype('category')
        df_a_ser_transformado['Região/Bacia'] = df_a_ser_transformado['Região/Bacia'].astype('category')
        df_a_ser_transformado['Natureza do projeto'] = df_a_ser_transformado['Natureza do projeto'].astype('category')
        df_a_ser_transformado['Tipo de locação'] = df_a_ser_transformado['Tipo de locação'].astype('category')
        df_a_ser_transformado['SICAR'] = df_a_ser_transformado['SICAR'].astype('category')
        df_a_ser_transformado['Rodada ANP'] = df_a_ser_transformado['Rodada ANP'].astype('category')
        df_a_ser_transformado['Demandante'] = df_a_ser_transformado['Demandante'].astype('category')
        df_a_ser_transformado['Tipo de completação inferior'] = df_a_ser_transformado['Tipo de completação inferior'].astype('category')

        df_com_dados_categoricos = df_a_ser_transformado.copy()
        del df_com_dados_categoricos['Ponto geográfico']
        return df_com_dados_categoricos

    def transformaDadosCategoricosEmDadosNumericos(self, df_categorico):
        cat_columns = df_categorico.select_dtypes(['category']).columns

        df_numerico = df_categorico.copy()

        for col in cat_columns:
            dummies = pd.get_dummies(df_numerico[col], prefix=col)
            df_numerico = pd.concat([df_numerico, dummies], axis=1)
            df_numerico.drop(col, axis=1, inplace=True)

        #convertendo colunas bool em int
        df_numerico = df_numerico * 1

        return df_numerico

    # rotina para tratar completamente o dataframe apos ser lido e carregado atraves do metodo importaPlanilhaEGeraDataFrameComDados
    def trataDataFrame(self, df_a_ser_tratado):
        df_somente_perfuracao = self.mantemApenasAtividadesPerfuracao(df_a_ser_tratado)
        df_ajustado = self.geraDataFrameAgrupadoPorPoco(df_somente_perfuracao)
        df_categorico = self.transformaDadosCategoricos(df_ajustado)
        df_numerico = self.transformaDadosCategoricosEmDadosNumericos(df_categorico)
        return df_numerico
