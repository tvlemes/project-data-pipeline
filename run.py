import click
from src.aquisicao.opcoes import EnumETL
from src.aquisicao.opcoes import ETL_DICT
import utils.configs as conf_geral

@click.group()
def cli():
        pass

@cli.group()
def aquisicao():
        """
        Grupo de comandos que executa as funções de aquisição
        """
        pass

@aquisicao.command()
@click.option('--etl', type=click.Choice([s.value for s in EnumETL]), help="Nome do ETL a ser executa.")
@click.option('--entrada', default=conf_geral.PASTA_DADOS, help='String com caminho para pasta de entrada.')
@click.option('--saida', default=conf_geral.PASTA_SAIDA_AQUISICAO, help='String com caminho para pasta de saída.')
@click.option('--criar_caminho', default=True, help='Flag indicando se devemos criar os caminhos.')
def processa_dado(
        etl:            str,
        entrada:        str,
        saida:          str,
        criar_caminho:  bool,
    ) -> None:
        """
        Executa o pipeline de uma determinada fonte.
        
        :param etl: Nome do ETL a ser executa.
        :param entrada: String com caminho para pasta de entrada.
        :param saida: String com caminho para pasta de saída.
        :param criar_caminho: Flag indicando se devemos criar os caminhos.
        """
        objeto = ETL_DICT[EnumETL(etl)](entrada, saida, criar_caminho)
        object.pipeline()

if __name__ == "__main__":
        cli()