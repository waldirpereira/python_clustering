import pandas as pd
import numpy as np
import sys
import os.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from transformacaoDadosService import TransformacaoDadosService

# https://www.spyder-ide.org/blog/introducing-unittest-plugin/

_transformacaoDadosService = TransformacaoDadosService()

def test_importaPlanilhaEGeraDataFrameComDadosDeveriaImportarPlanilha():
    # given
    meuDiretorio = os.path.dirname(__file__)
    caminhoPlanilha = os.path.join(meuDiretorio, 'entrada.xlsx')
    nomeAba = 'Dados'    
    # when
    df_importada = _transformacaoDadosService.importaPlanilhaEGeraDataFrameComDados(caminhoPlanilha, nomeAba)
    
    # then
    assert df_importada.shape[0] == 657


def test_mantemApenasAtividadesPerfuracaoDeveriaManterApenasAtividadesDePerfuracaoNaColunaTipoServicoDeDataFrame():
    # given 
    data = [{'Tipo de serviço': 'Perfuração', 'Id': 1},
            {'Tipo de serviço': 'Outra', 'Id': 2},
            {'Tipo de serviço': 'Perfuração', 'Id': 3},
            {'Tipo de serviço': 'Completação', 'Id': 4},
            {'Tipo de serviço': 'Perfuraçãoooo', 'Id': 5},
            {'Tipo de serviço': 'Perf', 'Id': 6}
        ]
    df = pd.DataFrame(data)
    
    # when
    df_somente_perfuracao = _transformacaoDadosService.mantemApenasAtividadesPerfuracao(df)

    # then
    assert df_somente_perfuracao.shape[0] == 2
    assert df_somente_perfuracao.iloc[0]['Tipo de serviço'] == 'Perfuração'
    assert df_somente_perfuracao.iloc[1]['Tipo de serviço'] == 'Perfuração'


def test_geraDataFrameAgrupadoPorPocoDeveriaAgruparPorPoco():
    def criaLinhaParaDataFrame(nomePoco, index):
        return pd.DataFrame([{
            'Ponto geográfico': nomePoco,
            'Projeto': 'Projeto {}'.format(index),
            'Atividade contingente': False,
            'Duração planejada': index,
            'Coordenador CIPP': '',
            'Ativo': '',
            'Bloco/Campo': '',
            'Tipo de óleo': '',
            'Região/Bacia': '',
            'Natureza do projeto': '',
            'Tipo de locação': '',
            'Projeto pré-sal': '',
            'Cessão onerosa': '',
            'SICAR': '',
            'Rodada ANP': '',
            'Formação': 'CARBONATO',
            'LDA': '',
            'Demandante': '',
            'Tipo de completação inferior': '',
            'Demanda CATS': 'Não aplicável',
            'Necessidade de SCC': 'Não'
        }])

    # given
    df = pd.DataFrame()
    
    df = df.append(criaLinhaParaDataFrame('PG1', 1))
    df = df.append(criaLinhaParaDataFrame('PG2', 2))
    df = df.append(criaLinhaParaDataFrame('PG3', 3))
    df = df.append(criaLinhaParaDataFrame('PG4', 4))
    df = df.append(criaLinhaParaDataFrame('PG1', 5))
    df = df.append(criaLinhaParaDataFrame('PG2', 6))
    df = df.append(criaLinhaParaDataFrame('PG2', 7))
    df = df.append(criaLinhaParaDataFrame('PG3', 8))
            
    # when
    df_agrupado= _transformacaoDadosService.geraDataFrameAgrupadoPorPoco(df)
    
    # then
    assert df_agrupado.shape[0] == 4
    assert df_agrupado.iloc[0]['Ponto geográfico'] == 'PG1'
    assert df_agrupado.iloc[1]['Ponto geográfico'] == 'PG2'
    assert df_agrupado.iloc[2]['Ponto geográfico'] == 'PG3'
    assert df_agrupado.iloc[3]['Ponto geográfico'] == 'PG4'


def test_transformaDadosCategoricosDeveriaManterApenasDadosCategoricos():
    def criaLinhaParaDataFrame(nomePoco, index):
        return pd.DataFrame([{
            'Ponto geográfico': nomePoco,
            'Projeto': 'Projeto {}'.format(index),
            'Atividade contingente': False,
            'Duração planejada': index,
            'Coordenador CIPP': 'CIPP {}'.format(index),
            'Ativo': 'Ativo {}'.format(index),
            'Bloco/Campo': 'Bloco {}'.format(index),
            'Tipo de óleo': 'TipoOleo {}'.format(index),
            'Região/Bacia': 'Bacia {}'.format(index),
            'Natureza do projeto': 'Natureza {}'.format(index),
            'Tipo de locação': 'TipoLocacao {}'.format(index),
            'Projeto pré-sal': 'Sim',
            'Cessão onerosa': 'Não',
            'SICAR': 'SICAR {}'.format(index),
            'Rodada ANP': 'Rodada {}'.format(index),
            'Formação': 'Formacao {}'.format(index),
            'LDA': 1000,
            'Demandante': 'Demandante {}'.format(index),
            'Tipo de completação inferior': 'TipoCompletacaoInferior {}'.format(index),
            'Demanda CATS': 'CATS {}'.format(index),
            'Necessidade de SCC': 'Não'
        }])

    # given
    df = pd.DataFrame()
    
    df = df.append(criaLinhaParaDataFrame('PG1', 1))
    df = df.append(criaLinhaParaDataFrame('PG2', 2))
    df = df.append(criaLinhaParaDataFrame('PG3', 3))
    df = df.append(criaLinhaParaDataFrame('PG4', 4))
    df = df.append(criaLinhaParaDataFrame('PG1', 5))
    df = df.append(criaLinhaParaDataFrame('PG2', 6))
    df = df.append(criaLinhaParaDataFrame('PG2', 7))
    df = df.append(criaLinhaParaDataFrame('PG3', 8))
    
    # then
    # checando que as colunas atualmente sao nao-categoricas
    assert df.select_dtypes(include='category').shape[1] == 0
    assert df['Projeto'].dtype.name != 'category'
    assert df['Coordenador CIPP'].dtype.name != 'category'
    assert df['Ativo'].dtype.name != 'category'
    assert df['Bloco/Campo'].dtype.name != 'category'
    assert df['Tipo de óleo'].dtype.name != 'category'
    assert df['Região/Bacia'].dtype.name != 'category'
    assert df['Natureza do projeto'].dtype.name != 'category'
    assert df['Tipo de locação'].dtype.name != 'category'
    assert df['SICAR'].dtype.name != 'category'
    assert df['Rodada ANP'].dtype.name != 'category'
    assert df['Demandante'].dtype.name != 'category'
    assert df['Tipo de completação inferior'].dtype.name != 'category'
    
    # when
    df_categorico = _transformacaoDadosService.transformaDadosCategoricos(df)
    
    # then
    # checando que as colunas atualmente sao categoricas
    assert df_categorico.select_dtypes(include='category').shape[1] > 0
    assert df['Projeto'].dtype.name == 'category'
    assert df['Coordenador CIPP'].dtype.name == 'category'
    assert df['Ativo'].dtype.name == 'category'
    assert df['Bloco/Campo'].dtype.name == 'category'
    assert df['Tipo de óleo'].dtype.name == 'category'
    assert df['Região/Bacia'].dtype.name == 'category'
    assert df['Natureza do projeto'].dtype.name == 'category'
    assert df['Tipo de locação'].dtype.name == 'category'
    assert df['SICAR'].dtype.name == 'category'
    assert df['Rodada ANP'].dtype.name == 'category'
    assert df['Demandante'].dtype.name == 'category'
    assert df['Tipo de completação inferior'].dtype.name == 'category'
    
def test_transformaDadosCategoricosEmDadosNumericosDeveriaTransformarColunasCategoricasEmNumericas():
    # given
    df = pd.DataFrame([
            {'COL_CAT': 'teste1', 'COL_NUM': 1, 'COL_BOOL': True},
            {'COL_CAT': 'teste2', 'COL_NUM': 2, 'COL_BOOL': True},
            {'COL_CAT': 'teste3', 'COL_NUM': 3, 'COL_BOOL': True},
            {'COL_CAT': 'teste4', 'COL_NUM': 4, 'COL_BOOL': True}
        ])
    
    df['COL_CAT'] = df['COL_CAT'].astype('category')
    
    # then
    assert 'COL_CAT' in df
    assert df['COL_CAT'].dtype.name == 'category'
    
    # when
    df_numerico = _transformacaoDadosService.transformaDadosCategoricosEmDadosNumericos(df)
    
    # then
    # checando que a coluna COL_CAT nao existe mais
    assert 'COL_CAT' not in df_numerico
    
    #checando que as colunas categoricas foram criadas e sao numericas
    assert np.issubdtype(df_numerico['COL_CAT_teste1'].dtype, np.number)
    assert np.issubdtype(df_numerico['COL_CAT_teste2'].dtype, np.number)
    assert np.issubdtype(df_numerico['COL_CAT_teste3'].dtype, np.number)
    assert np.issubdtype(df_numerico['COL_CAT_teste4'].dtype, np.number)    
