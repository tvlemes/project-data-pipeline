import abc 
import os
import typing
import urllib
from bs4 import BeautifulSoup
from src.aquisicao.inep.base_inep import BaseINEPETL
from src.utils.web import download_dados_web
import zipfile
import pandas as pd

class BaseCensoEscolarTETL(BaseINEPETL, abc.ABC):
    """
    Classe que estrutura como qualquer objeto de ETL do Censo Escolar
    deve funcionar para baixar dados do CensoEscola
    """
    _tabela: str

    def __init__(
            self,
            entrada: str,
            saida: str,
            tabela: str,
            criar_caminho: bool = True,
    ) -> None:
        """
        Instância o objeto de ETL Base.
        :param entrada: String com caminho para pasta de entrada.
        :param saida: String com caminho para pasta de saída.
        :param tabela: Tabela do censo escolar a ser processada.
        :param criar_caminho: Flag indicando se devemos criar os caminhos.
        """

        super().__init__(entrada, saida, "censo-escolar", criar_caminho)

        self.tabela = tabela

    def le_pagina_inep(self) -> typing.Dict[str, str]:
        """
        Realiza o Web-Scraping da página INEP.

        :return: Nome do aquivo e link para a página
        """
        html = urllib.request.urlopen(self._url).read()

        soup = BeautifulSoup(html, features='html.parser')

        return {tag['href'].split("_")[-1]: tag['href'] for tag in soup.find_all("a", {"class":"external-link"})}

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
        for arq, link in self.dicionario_para_baixar().items():
            # print(f'Baixando o arquivo: {arq}') # Verifica se está baixando o arquivo
            caminho_arq = self.caminho_saida / arq
            download_dados_web(caminho_arq, link)


    def extract(self) -> None:
        """
        Extraí os dados do objeto.
        """
        # Realiza o download dos dados do Censo Escolar
        self.download_conteudo()

        # Carrega as tabelas de interesse
        for arq in self.le_pagina_inep():
            with zipfile.ZipFile(arq) as arq_zip:
                nome_zip = [arq for arq in arq_zip.namelist() if self._tabela in arq]
                self._dados_entrada[arq] = pd.read_cs(
                    arq_zip.open(nome_zip), encoding="latin-1", sep='|'
                )
    @abc.abstractmethod
    def transform(self) -> None:
        """
        Transforma os dados e os adequa para os formatos de
        saída de interesse.
        """
        pass
