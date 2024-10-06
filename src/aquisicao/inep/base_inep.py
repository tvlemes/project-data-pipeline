import abc  # cria a estrutura base para ETL
import os
import typing
import urllib
from bs4 import BeautifulSoup
from src.aquisicao.base_etl import BaseETL
from src.utils.web import download_dados_web

class BaseINEPETL(BaseETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL
    deve funcionar para baixar dados do INEP
    """

    URL = str = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar"

    def __init__(
            self,
            entrada: str,
            saida: str,
            base: str,
            criar_caminho: bool = True,
    ) -> None:
        """
        Instância o objeto de ETL Base.
        :param entrada: string com caminho para pasta de entrada.
        :param saida: string com caminho para pasta de saída.
        :param url: URL para a página de dados do INEP.
        :param criar_caminho: flag indicando se devemos criar os caminhos.
        :param reprocessar: flag para forçar o re-processamento das bases de dados.
        """

        super().__init__(entrada, saida, criar_caminho)
        self._url = f"{self.URL}/{base}"

    def le_pagina_inep(self) -> typing.Dict[str, str]:
        """
        Realiza o Web-Scraping da página INEP.

        :return: No do aquivo e link para a página
        """
        html = urllib.request.urlopen(self._url).read()
        soup = BeautifulSoup(html, features='html.parser')

        # Criando um dicionário com o nome das URL´s para download
        #####################################################################################################
        # Capturando a class no arquivo HTML cujo nome é external-link 
        # tags = soup.find_all("a", {"class":"external-link"})
        # tag = tags[0]
        # print(tag['href'])

        # Outra forma: {'2023.zip': 'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2023.zip', 
        #               '2022.zip': 'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2022.zip'...}
        links = {tag['href'].split("_")[-1]: tag['href'] for tag in soup.find_all("a", {"class":"external-link"})}

    def dicionario_para_baixar(self) -> typing.Dict[str, str]:

        """
        Lê os conteúdos da pasta de dados e seleciona apenas os arquivos
        a serem baixados como complementares.

        :return: Dicionário com nome do arquivo e link para a página.   
        """

        para_baixar = self.le_pagina_inep()
        baixados = os.listdir(str(self.caminho_entrada))
        return {arq: link for arq, link in para_baixar.items() if arq not in baixados}

    def download_conteudo(self) -> None:
        """
        Realiza os download dos dados do INEP para uma pasta local.
        """
        for arq, link in self.dicionario_para_baixar():
            caminho_arq = self.caminho_saida / arq
            download_dados_web(caminho_arq, link)


    @abc.abstractmethod
    def extract(self) -> None:
        """
        Extraí os dados do objeto.
        """
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse.
        """
        pass

    def load(self) -> None:
        """
        Exporta os dados transformados.
        """
        for arq, df in self.dados_saida.items():
            df.to_parquet(self.caminho_saida / arq, index=False)

    def pipeline(self) -> None:
        """
        Executa o pipeline completo de tratamento de dados.
        """
        self.extract()
        self.transform()
        self.load()
