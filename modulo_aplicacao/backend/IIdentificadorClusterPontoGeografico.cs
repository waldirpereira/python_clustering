using CronogramaDeSondas.Dominio.Modelos.Cronograma.Foto;

namespace CronogramaDeSondas.Infra.Python
{
    public interface IIdentificadorClusterPontoGeografico
    {
        string DescobreCluster(FotoPontoGeografico fotoPontoGeografico);
    }
}