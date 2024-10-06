import requests
from pathlib import Path
import typing

def download_dados_web(
        caminho: typing.Union[str, Path],
        url: str = str
) -> None:
    '''
    Realiza o download dos dados em um link da Web.

    :param caminho: Caminho para extração dos dados.\n
    :param url: endereço do site do site a ser baixado. \n
    '''

    r = requests.get(url, stream=True)
    with open(caminho, "wb") as arq:
        arq.write(r.content)