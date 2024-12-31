import requests
import re
import json
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from typing import Union

# Importações para retries
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Se quiser converter algarismos romanos para números, instale e importe a biblioteca 'roman':
# pip install roman
try:
    import roman
except ImportError:
    roman = None

def search_regex(pattern: str, text: str) -> Union[str, None]:
    """
    Retorna a primeira correspondência (group(0)) de 'pattern' em 'text',
    ou None se não houver match.
    """
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return match.group(0) if match else None

def extract_roman_number(pattern: str, text: str) -> Union[int, str, None]:
    """
    Aplica 'search_regex' para encontrar sequência romana (ex.: 'I', 'II', 'III').
    Se 'roman' estiver instalado, converte para inteiro.
    Se não, retorna a string encontrada ou None se não houve match.
    """
    found = search_regex(pattern, text)
    if not found:
        return None
    if roman:
        try:
            return roman.fromRoman(found.upper())  # .upper() para garantir
        except:
            pass
    return found

def scrap_constitution_planalto(output_file: str) -> None:
    """
    Lê o HTML da Constituição Federal em:
        https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm
    Remove texto revogado (<strike>), extrai a estrutura hierárquica
    (Título, Capítulo, Seção, Subsecção, Artigo, etc.) e salva em 'output_file' (JSON).
    """

    # URL do Planalto
    url = "https://www.planalto.gov.br/ccivil_03/constituicao/constituicao.htm"

    # -----------------------------------------------------------
    # 1) Baixa o HTML com retries e backoff
    # -----------------------------------------------------------
    # Cria uma Session para configurar retries
    session = requests.Session()

    # Configura política de retries:
    #   - total=5: até 5 tentativas
    #   - backoff_factor=1: espera progressiva (1s, 2s, 4s...) entre tentativas
    #   - status_forcelist=[429, 500, 502, 503, 504]: códigos de status que acionam retry
    #   - method_whitelist=["HEAD", "GET", "OPTIONS"]: quais métodos podem sofrer retry
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # para versões mais recentes: 'allowed_methods'
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Não foi possível baixar a Constituição do Planalto: {e}")
        return

    html_content = response.text

    # -----------------------------------------------------------
    # 2) Remove <strike> (conteúdo revogado)
    # -----------------------------------------------------------
    soup = BeautifulSoup(html_content, "lxml")

    # Remove qualquer conteúdo dentro de <strike> (revogado)
    for strike_tag in soup.find_all("strike"):
        strike_tag.decompose()  # remove do DOM

    # -----------------------------------------------------------
    # 3) Captura parágrafos <p> do corpo do texto
    # -----------------------------------------------------------
    paragraphs = soup.find_all("p")

    # Se não achar nada, provavelmente a estrutura do site mudou
    if not paragraphs:
        print("Não foram encontrados parágrafos <p> no HTML. Verifique se o layout mudou.")
        return

    # Monta um DataFrame com todos os parágrafos
    data_rows = []
    for p in paragraphs:
        text = p.get_text(" ", strip=True)  # separa por espaços, remove \n
        # Ignora parágrafos vazios
        if text.strip():
            data_rows.append({"texto": text.strip()})

    df = pd.DataFrame(data_rows)
    if df.empty:
        print("Nenhum parágrafo com conteúdo foi encontrado após remover <strike>.")
        return

    # Remove espaços duplicados
    df["texto"] = df["texto"].apply(lambda x: re.sub(r"\s+", " ", x).strip())

    # -----------------------------------------------------------
    # 4) Extrair cada nível hierárquico usando regex
    # -----------------------------------------------------------
    regex_map_roman = {
        "titulo": r"(?<=^T[ÍI]TULO\s*)[IVX]+",   # TÍTULO I, TÍTULO II...
        "capitulo": r"(?<=^CAP[IÍ]TULO\s*)[IVX]+",
        "secao": r"(?<=^SEÇÃO\s*)[IVX]+",
        "subsecao": r"(?<=^SUBSEÇÃO\s*)[IVX]+"
    }

    regex_map_generic = {
        "artigo": r"(?<=^Art\.?\s*)[0-9A-Zº\-]+",  # Art. 5º, Art 6º...
        "paragrafo": r"(?<=^§\s*)[0-9A-Zº\-]+",    # § 1º, § 2º...
    }

    # Extrai Título, Capítulo, Seção, Subseção
    for col, pattern in regex_map_roman.items():
        df[col] = df["texto"].apply(lambda x: extract_roman_number(pattern, x.upper()))

    # Extrai Artigo e Parágrafo
    for col, pattern in regex_map_generic.items():
        df[col] = df["texto"].apply(lambda x: search_regex(pattern, x))

    # Parágrafo único
    def handle_paragrafo(row_text, current):
        """
        Se começa com "Parágrafo único", marcamos como 'único'.
        """
        pattern_unique = r"^Parágrafo\s+único"
        if re.search(pattern_unique, row_text, flags=re.IGNORECASE):
            return "único"
        return current

    df["paragrafo"] = df.apply(lambda row: handle_paragrafo(row["texto"], row["paragrafo"]), axis=1)

    # Incisos (ex.: I - ...), podem estar no início da linha
    df["inciso"] = df["texto"].apply(lambda x: search_regex(r"^[IVXLA-Z\d]+(?=\s*-)"))
    # Se for "VIX", converte para "IX"
    df.loc[df["inciso"] == "VIX", "inciso"] = "IX"

    # Alínea (ex.: "a)", "b)"), no início da linha
    df["alinea"] = df["texto"].apply(lambda x: search_regex(r"^[a-z]\)"))

    # -----------------------------------------------------------
    # 5) "Preencher" as colunas hierárquicas (ffill)
    # -----------------------------------------------------------
    hierarchical_cols = [
        "titulo", "capitulo", "secao", "subsecao",
        "artigo", "paragrafo", "inciso", "alinea"
    ]
    df[hierarchical_cols] = df[hierarchical_cols].ffill()

    # -----------------------------------------------------------
    # 6) Converte DataFrame em uma estrutura aninhada (JSON)
    # -----------------------------------------------------------
    def add_line(root_dict, row_dict):
        """
        Insere a linha do DF em root_dict, seguindo a hierarquia:
          root_dict -> 'titulos' -> <valor_do_titulo> -> 'capitulos' -> ...
        """

        map_keys = {
            "titulo": "titulos",
            "capitulo": "capitulos",
            "secao": "secoes",
            "subsecao": "subsecoes",
            "artigo": "artigos",
            "paragrafo": "paragrafos",
            "inciso": "incisos",
            "alinea": "alineas"
        }

        # Determina a "chave" final (numero), a última coluna não-vazia
        used_cols = [c for c in hierarchical_cols if row_dict.get(c) is not None]
        numero_col = used_cols[-1] if used_cols else None
        numero_str = row_dict.get(numero_col) if numero_col else None

        # Para debug, podemos deduzir uma "classe" textual
        classe = numero_col if numero_col else "texto"
        texto = row_dict["texto"]

        entry = {
            "classe": classe,
            "numero": numero_str,
            "texto": texto
        }

        current_level = root_dict
        for col in hierarchical_cols:
            val = row_dict.get(col)
            if val is None:
                break
            plural_key = map_keys[col]
            if plural_key not in current_level:
                current_level[plural_key] = {}
            if val not in current_level[plural_key]:
                current_level[plural_key][val] = {}
            current_level = current_level[plural_key][val]

        if "conteudo" not in current_level:
            current_level["conteudo"] = []
        current_level["conteudo"].append(entry)

    root = {}
    for _, row in df.iterrows():
        add_line(root, row.to_dict())

    # -----------------------------------------------------------
    # 7) Salva em JSON
    # -----------------------------------------------------------
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(root, f, ensure_ascii=False, indent=2)

    print(f"Constituição coletada do Planalto e salva em: {output_file}")


# ---------------------------------------------------------------------
# Exemplo de uso (main)
# ---------------------------------------------------------------------
if __name__ == "__main__":
    output_json = "br_constitution.json"
    scrap_constitution_planalto(output_json)
