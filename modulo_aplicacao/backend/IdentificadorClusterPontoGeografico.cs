using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using CronogramaDeSondas.Dominio.Modelos;
using CronogramaDeSondas.Dominio.Modelos.Cronograma.Filme;
using CronogramaDeSondas.Dominio.Modelos.Cronograma.Foto;
using CronogramaDeSondas.Dominio.Modelos.Sonda;
using CronogramaDeSondas.Infra.ExportacaoPlanilha.Cronograma;
using MongoDB.Bson;
using Newtonsoft.Json;
using PyRunner;

namespace CronogramaDeSondas.Infra.Python
{
    public class IdentificadorClusterPontoGeografico : IIdentificadorClusterPontoGeografico
    {
        private PythonRunner PythonRunner { get; }

        private class PropriedadeParaArgumento<TP>
        {
            public Func<TP, object> PropriedadeValor { get; set; }
            public string NomeColuna { get; set; }
        }

        public IdentificadorClusterPontoGeografico()
        {
            //var pythonRunner = new PythonRunner(ConfigurationManager.AppSettings["pythonPath"], 20000);
            PythonRunner = new PythonRunner("D:\\ProgramData\\Anaconda3\\python.exe", 20000);
        }

        private string PegaCaminhoScript()
        {
            //var codeBase = Assembly.GetAssembly(typeof(IdentificadorClusterPontoGeografico)).CodeBase;
            var location = Assembly.GetExecutingAssembly().Location;
            var estaRodandoTeste = location.Contains("TestResults");
            var caminho = estaRodandoTeste ? location : Assembly.GetAssembly(typeof(IdentificadorClusterPontoGeografico)).CodeBase;
            return Path.Combine(
                Path.GetDirectoryName(caminho) ?? throw new InvalidOperationException(),
                estaRodandoTeste ? string.Empty : "Python\\Scripts\\");
        }

        public string DescobreCluster(FotoPontoGeografico fotoPontoGeografico)
        {
            var caminhoScript = PegaCaminhoScript().Replace("file:\\", "");

            var tipoServico = new TipoServico
            {
                Codigo = TipoServico.CodigoTipoServicoPerfuracao,
                Nome = "Perfuração"
            };

            var fotoTarefa = new FotoTarefa
            {
                FotoPontoGeografico = fotoPontoGeografico,
                TipoTarefa = new TipoTarefa { Nome = "TT" },
                FotoServico = new FotoServico
                {
                    Nome = "SRV",
                    TipoServico = tipoServico,
                    FotoLocacao = new FotoLocacao
                    {
                        Nome = "LOC",
                        CodigoLocacaoFilme = 1,
                        WibrNovosPadsNovosBids = 1,
                        Bloco = new Bloco
                        {
                            Nome = "BLOCO",
                            Ativo = new Ativo
                            {
                                Sigla = "ATIVO"
                            },
                            Rodada = new Rodada
                            {
                                Id = Rodada.CodigoCessaoOnerosa
                            }
                        },
                        FotoProjeto = new FotoProjeto
                        {
                            Nome = "PRJ",
                            UnidadeOperativa = new UnidadeOperativa { Nome = "UO" },
                            ChaveUsuarioCaCipp = "CIPP",
                            ProjetoNovosPadsNovosBids = true,
                            GrupoEditorResponsavelPeloProjeto = new GrupoEditor
                            {
                                Codigo = GrupoEditor.CodigoGrupoAgp,
                                Nome = "AGP"
                            }
                        }
                    }
                }
            };

            var fotoRecurso = new FotoSonda
            {
                Nome = "SONDA",
                Prefixo = "SND",
                StatusContratacao = StatusContratacao.Ativo,
                PosicionamentoSonda = PosicionamentoSonda.Ancorado,
                TipoRecurso = TipoRecurso.Sonda
            };

            var fotoAlocacaoTarefa = new FotoAlocacaoTarefa
            {
                FotoTarefa = fotoTarefa,
                FotoRecurso = fotoRecurso,
                FotoCronograma = new FotoCronograma
                {
                    DataDaFoto = DateTime.Today
                }
            };

            var escopoAtividade = new FilmeEscopoAtividade
            {
                Atividade = new Atividade
                {
                    Codigo = "A999999",
                    Nome = "Atividade",
                    TipoAtividade = new TipoAtividade
                    {
                        Codigo = TipoAtividade.CodigoTipoAtividadePerfuracao,
                        Nome = "TA",
                        TipoServico = tipoServico
                    }
                },
                FilmeEscopo = new FilmeEscopo(),
                Fase = 1,
                Duracao = 1,
                EhContingencial = false,
                DataInicioCalculada = DateTime.Now
            };

            var tupla = new TuplaEscopoAtividadeFotoAlocacaoTarefaParaPlanilhaEscopoIntegracaoDelfos(fotoAlocacaoTarefa,
                new CodigoUnico { Codigo = "CODIGO_UNICO" }, escopoAtividade);

            var dic = new Dictionary<string, object>();
            dic.Add("Prefixo do recurso", tupla.PrefixoRecurso);
            dic.Add("Nome do recurso", tupla.NomeRecurso);
            dic.Add("Código único", tupla.CodigoUnico);
            dic.Add("Projeto", tupla.NomeProjeto);
            //dic.Add("UO do projeto", tupla.SiglaUoProjeto);
            //dic.Add("ID da Locação", tupla.CodigoLocacaoFilme);
            dic.Add("Locações (Subprojeto)", tupla.NomeSubProjeto);
            //dic.Add("ID do Poço", tupla.CodigoPoco);
            dic.Add("Ponto geográfico", tupla.NomePontoGeografico);
            dic.Add("Tipo de serviço", tupla.TipoServico);
            dic.Add("Subtipo de serviço", tupla.SubtipoServico);
            dic.Add("Tipo de atividade", tupla.TipoAtividade);
            dic.Add("Nome da atividade", tupla.NomeAtividade);
            dic.Add("Fase da atividade", tupla.FaseAtividade);
            dic.Add("Atividade contingente", tupla.AtividadeContingente);
            dic.Add("Duração planejada", tupla.Duracao);
            dic.Add("Duração realizada", tupla.DuracaoRealizada);
            dic.Add("Início", tupla.Inicio);
            dic.Add("Término", tupla.Termino);
            dic.Add("Coordenador CIPP", tupla.CoordenadorCipp);
            dic.Add("Ativo", tupla.Ativo);
            dic.Add("Bloco/Campo", tupla.BlocoCampo);
            dic.Add("Tipo de óleo", tupla.TipoOleo);
            dic.Add("Poço POLEO", tupla.PocoPoleo);
            dic.Add("Projeto de investimento", tupla.ProjetoInvestimento);
            dic.Add("Região/Bacia", tupla.Bacia);
            dic.Add("Natureza do projeto", tupla.NomeNaturezaProjeto);
            dic.Add("Tipo de locação", tupla.TipoLocacao);
            dic.Add("Projeto pré-sal", tupla.ProjetoPreSal);
            dic.Add("Cessão onerosa", tupla.CessaoOnerosa);
            dic.Add("SICAR", tupla.Sicar);
            dic.Add("Parcerias", tupla.Parcerias);
            dic.Add("Fase do projeto", tupla.FaseProjeto);
            dic.Add("Rodada ANP", tupla.RodadaAnp);
            dic.Add("Pressão de trabalho do poço", tupla.PressaoTrabalhoPoco);
            dic.Add("Classe de pressão e temperatura do poço", tupla.ClassePressaoTemperatura);
            dic.Add("Formação", tupla.Formacao);
            dic.Add("LDA", tupla.Lda);
            dic.Add("Início de poço", tupla.InicioPoco);
            dic.Add("Poço-tipo", tupla.PocoTipo);
            dic.Add("Subtipo de poço", tupla.SubtipoPoco);
            dic.Add("ID do projeto", tupla.IdProjeto);
            dic.Add("ID da atividade", tupla.IdAtividade);
            dic.Add("Demandante", tupla.Demandante);
            dic.Add("Coordenador do projeto no cliente", tupla.CoordenadorProjetoCliente);
            dic.Add("Geometria do poço", tupla.GeometriaPoco);
            dic.Add("Diâmetro do packer", tupla.DiametroPacker);
            dic.Add("Tipo de completação inferior", tupla.TipoCompletacaoInferior);
            dic.Add("Demanda CATS", tupla.DemandaCats);
            dic.Add("Necessidade de SCC", tupla.NecessidadeScc);
            dic.Add("ID Tarefa original", tupla.CodigoDaTarefaFilmeOriginal);
            dic.Add("Porto de embarque", tupla.PortoEmbarque);
            dic.Add("IUPI do projeto de investimento", tupla.IupiProjetoInvestimento);
            dic.Add("Tipo de ponto geográfico", tupla.TipoPontoGeografico);
            dic.Add("Tipo de tarefa", tupla.TipoTarefa);
            dic.Add("Tarefa probabilística/determinística", tupla.IndicadorProbabilisticoDeterministico);
            dic.Add("Prefixo do recurso na base integrada", tupla.PrefixoBaseIntegrada);
            dic.Add("Status de contratação do recurso", tupla.StatusContratacaoRecurso);
            dic.Add("Tipo de recurso", tupla.TipoRecurso);
            dic.Add("Subtipo de recurso", tupla.SubtipoRecurso);
            dic.Add("Tipo posicionamento do recurso", tupla.TipoPosicionamentoRecurso);

            string json = JsonConvert.SerializeObject(dic);
            json = json.Replace("\"", "\\\"");
            var output = PythonRunner.Execute($"{caminhoScript}\\identificador-cluster.py", json, caminhoScript);
            return output;
        }

        public async Task<string> Soma(int a, int b)
        {
            var output = await PythonRunner.ExecuteAsync($"{PegaCaminhoScript()}\\identificador-cluster.py", a, b);
            return output;
        }
    }
}
